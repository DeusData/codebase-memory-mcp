#include "foundation/arena.h" /* public lazy-arena declarations before cbm.h */
/*
 * userconfig.c — User-defined extension→language mappings.
 *
 * Reads extra_extensions from:
 *   Global:  $XDG_CONFIG_HOME/codebase-memory-mcp/config.json
 *            (falls back to ~/.config/codebase-memory-mcp/config.json)
 *   Project: {repo_root}/.codebase-memory.json
 *
 * Project config wins over global. Unknown language values warn and are
 * skipped (fail-open). Missing files are silently ignored.
 */
#include "discover/userconfig.h"
#include "cbm.h" /* CBMLanguage, CBM_LANG_* */
#include "foundation/constants.h"
#include "foundation/platform.h" /* cbm_safe_getenv */
#include "foundation/compat_fs.h"
#include "foundation/sha256.h"
#include "foundation/arena.h"

enum { MAX_CONFIG_SIZE = 65536 };
#include "foundation/log.h"

#include <yyjson/yyjson.h>

#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ── Process-global user config pointer ──────────────────────────── */

static const cbm_userconfig_t *g_userconfig = NULL;

struct cbm_userconfig_source {
    CBMArena arena;
    cbm_userconfig_source_state_t state;
    const char *bytes;
    size_t len;
    bool owns_config_arena; /* false for both legacy loaders */
};

static void userconfig_source_free(cbm_userconfig_source_t *source) {
    if (source) {
        CBMArena arena = source->arena;
        cbm_arena_destroy(&arena);
    }
}

static void userconfig_source_digest(const char *state, const void *bytes, size_t len,
                                     char out[CBM_SHA256_HEX_LEN + 1]) {
    static const char domain[] = "cbm-userconfig-source-v1";
    cbm_sha256_ctx sha;
    cbm_sha256_init(&sha);
    cbm_sha256_update(&sha, domain, sizeof(domain));
    cbm_sha256_update(&sha, state, strlen(state) + 1);
    if (bytes && len > 0) {
        cbm_sha256_update(&sha, bytes, len);
    }
    uint8_t digest[CBM_SHA256_DIGEST_LEN];
    cbm_sha256_final(&sha, digest);
    static const char hex[] = "0123456789abcdef";
    for (int i = 0; i < CBM_SHA256_DIGEST_LEN; i++) {
        out[i * 2] = hex[digest[i] >> 4];
        out[i * 2 + 1] = hex[digest[i] & 0x0f];
    }
    out[CBM_SHA256_HEX_LEN] = '\0';
}

void cbm_set_user_lang_config(const cbm_userconfig_t *cfg) {
    g_userconfig = cfg;
}

const cbm_userconfig_t *cbm_get_user_lang_config(void) {
    return g_userconfig;
}

/* ── Language name → enum table ──────────────────────────────────── */

/*
 * Reverse-mapping from lowercase language name strings to CBMLanguage.
 * Covers all names exposed by cbm_language_name() plus common aliases.
 */
typedef struct {
    const char *name; /* lowercase */
    CBMLanguage lang;
} lang_name_entry_t;

static const lang_name_entry_t LANG_NAME_TABLE[] = {
    {"go", CBM_LANG_GO},
    {"python", CBM_LANG_PYTHON},
    {"javascript", CBM_LANG_JAVASCRIPT},
    {"typescript", CBM_LANG_TYPESCRIPT},
    {"tsx", CBM_LANG_TSX},
    {"arkts", CBM_LANG_ARKTS},
    {"rust", CBM_LANG_RUST},
    {"java", CBM_LANG_JAVA},
    {"c++", CBM_LANG_CPP},
    {"cpp", CBM_LANG_CPP},
    {"c#", CBM_LANG_CSHARP},
    {"csharp", CBM_LANG_CSHARP},
    {"php", CBM_LANG_PHP},
    {"lua", CBM_LANG_LUA},
    {"scala", CBM_LANG_SCALA},
    {"kotlin", CBM_LANG_KOTLIN},
    {"ruby", CBM_LANG_RUBY},
    {"c", CBM_LANG_C},
    {"bash", CBM_LANG_BASH},
    {"sh", CBM_LANG_BASH},
    {"zig", CBM_LANG_ZIG},
    {"elixir", CBM_LANG_ELIXIR},
    {"haskell", CBM_LANG_HASKELL},
    {"ocaml", CBM_LANG_OCAML},
    {"objective-c", CBM_LANG_OBJC},
    {"objc", CBM_LANG_OBJC},
    {"swift", CBM_LANG_SWIFT},
    {"dart", CBM_LANG_DART},
    {"perl", CBM_LANG_PERL},
    {"groovy", CBM_LANG_GROOVY},
    {"erlang", CBM_LANG_ERLANG},
    {"r", CBM_LANG_R},
    {"html", CBM_LANG_HTML},
    {"css", CBM_LANG_CSS},
    {"scss", CBM_LANG_SCSS},
    {"yaml", CBM_LANG_YAML},
    {"toml", CBM_LANG_TOML},
    {"hcl", CBM_LANG_HCL},
    {"terraform", CBM_LANG_HCL},
    {"sql", CBM_LANG_SQL},
    {"dockerfile", CBM_LANG_DOCKERFILE},
    {"clojure", CBM_LANG_CLOJURE},
    {"f#", CBM_LANG_FSHARP},
    {"fsharp", CBM_LANG_FSHARP},
    {"julia", CBM_LANG_JULIA},
    {"vimscript", CBM_LANG_VIMSCRIPT},
    {"nix", CBM_LANG_NIX},
    {"common lisp", CBM_LANG_COMMONLISP},
    {"commonlisp", CBM_LANG_COMMONLISP},
    {"lisp", CBM_LANG_COMMONLISP},
    {"elm", CBM_LANG_ELM},
    {"fortran", CBM_LANG_FORTRAN},
    {"cuda", CBM_LANG_CUDA},
    {"cobol", CBM_LANG_COBOL},
    {"verilog", CBM_LANG_VERILOG},
    {"emacs lisp", CBM_LANG_EMACSLISP},
    {"emacslisp", CBM_LANG_EMACSLISP},
    {"json", CBM_LANG_JSON},
    {"xml", CBM_LANG_XML},
    {"markdown", CBM_LANG_MARKDOWN},
    {"makefile", CBM_LANG_MAKEFILE},
    {"cmake", CBM_LANG_CMAKE},
    {"protobuf", CBM_LANG_PROTOBUF},
    {"graphql", CBM_LANG_GRAPHQL},
    {"vue", CBM_LANG_VUE},
    {"svelte", CBM_LANG_SVELTE},
    {"meson", CBM_LANG_MESON},
    {"glsl", CBM_LANG_GLSL},
    {"ini", CBM_LANG_INI},
    {"matlab", CBM_LANG_MATLAB},
    {"mojo", CBM_LANG_MOJO},
    {"plsql", CBM_LANG_PLSQL},
    {"chialisp", CBM_LANG_CHIALISP},
    {"pdf", CBM_LANG_PDF},
    {"asciidoc", CBM_LANG_ASCIIDOC},
    {"lean", CBM_LANG_LEAN},
    {"form", CBM_LANG_FORM},
    {"magma", CBM_LANG_MAGMA},
    {"wolfram", CBM_LANG_WOLFRAM},
};

#define LANG_NAME_TABLE_SIZE (sizeof(LANG_NAME_TABLE) / sizeof(LANG_NAME_TABLE[0]))

/*
 * Parse a language string (case-insensitive) to a CBMLanguage enum.
 * Returns CBM_LANG_COUNT if the string is not recognized.
 */
static CBMLanguage lang_from_string(const char *s) {
    if (!s || !s[0]) {
        return CBM_LANG_COUNT;
    }

    /* Build a lowercase copy for comparison */
    char lower[CBM_SZ_64];
    size_t i;
    for (i = 0; i < sizeof(lower) - SKIP_ONE && s[i]; i++) {
        lower[i] = (char)tolower((unsigned char)s[i]);
    }
    lower[i] = '\0';

    for (size_t j = 0; j < LANG_NAME_TABLE_SIZE; j++) {
        if (strcmp(LANG_NAME_TABLE[j].name, lower) == 0) {
            return LANG_NAME_TABLE[j].lang;
        }
    }
    return CBM_LANG_COUNT;
}

/* ── Config directory helper ─────────────────────────────────────── */

/* cbm_app_config_dir() is now in platform.c (cross-platform). */

/* ── JSON parsing ────────────────────────────────────────────────── */

/*
 * Parse extra_extensions from a yyjson object root.
 * Appends valid entries to *entries / *count (growing via realloc).
 * Project-level entries (from_project=true) are appended after global
 * entries so that a later dedup pass can prefer project values.
 *
 * Returns 0 on success, -1 on alloc failure.
 */
static int parse_extra_extensions(yyjson_val *root, cbm_userext_t **entries, int *count,
                                  const char *source_label) {
    if (!yyjson_is_obj(root)) {
        cbm_log_warn("userconfig.bad_root", "file", source_label);
        return 0;
    }

    yyjson_val *extra = yyjson_obj_get(root, "extra_extensions");
    if (!extra) {
        return 0; /* key absent — fine */
    }
    if (!yyjson_is_obj(extra)) {
        cbm_log_warn("userconfig.bad_extra_extensions", "file", source_label);
        return 0;
    }

    yyjson_obj_iter iter;
    yyjson_obj_iter_init(extra, &iter);
    yyjson_val *key;
    while ((key = yyjson_obj_iter_next(&iter)) != NULL) {
        yyjson_val *val = yyjson_obj_iter_get_val(key);

        const char *ext_str = yyjson_get_str(key);
        const char *lang_str = yyjson_get_str(val);

        if (!ext_str || !lang_str) {
            cbm_log_warn("userconfig.skip_non_string", "file", source_label);
            continue;
        }

        /* Extension must start with '.' */
        if (ext_str[0] != '.') {
            cbm_log_warn("userconfig.skip_bad_ext", "file", source_label, "ext", ext_str);
            continue;
        }

        CBMLanguage lang = lang_from_string(lang_str);
        if (lang == CBM_LANG_COUNT) {
            cbm_log_warn("userconfig.unknown_lang", "file", source_label, "lang", lang_str);
            continue; /* fail-open: skip unknown languages */
        }

        /* Grow the array */
        cbm_userext_t *tmp = realloc(*entries, (size_t)(*count + SKIP_ONE) * sizeof(cbm_userext_t));
        if (!tmp) {
            return CBM_NOT_FOUND;
        }
        *entries = tmp;

        char *ext_copy = strdup(ext_str);
        if (!ext_copy) {
            return CBM_NOT_FOUND;
        }

        (*entries)[*count].ext = ext_copy;
        (*entries)[*count].lang = lang;
        (*count)++;
    }
    return 0;
}

/*
 * Read a JSON file and parse extra_extensions from it.
 * Silently ignores missing files. Logs warnings for corrupt JSON.
 * Returns 0 on success (or absent file), -1 on alloc failure.
 */
static int load_config_file(const char *path, cbm_userext_t **entries, int *count,
                            char source_sha256[CBM_SHA256_HEX_LEN + 1]) {
    userconfig_source_digest("missing-or-unreadable", NULL, 0, source_sha256);
    FILE *f = cbm_fopen(path, "rb");
    if (!f) {
        return 0; /* file absent — silently ignore */
    }

    if (fseek(f, 0, SEEK_END) != 0) {
        (void)fclose(f);
        userconfig_source_digest("seek-error", NULL, 0, source_sha256);
        return 0;
    }
    long len = ftell(f);
    if (fseek(f, 0, SEEK_SET) != 0) {
        (void)fclose(f);
        userconfig_source_digest("seek-error", NULL, 0, source_sha256);
        return 0;
    }

    if (len <= 0 || len > MAX_CONFIG_SIZE) {
        (void)fclose(f);
        if (len > MAX_CONFIG_SIZE) {
            cbm_log_warn("userconfig.file_too_large", "path", path);
            userconfig_source_digest("oversized", NULL, 0, source_sha256);
        } else {
            userconfig_source_digest("empty", NULL, 0, source_sha256);
        }
        return 0;
    }

    char *buf = malloc((size_t)len + SKIP_ONE);
    if (!buf) {
        (void)fclose(f);
        return CBM_NOT_FOUND;
    }

    size_t nread = fread(buf, SKIP_ONE, (size_t)len, f);
    (void)fclose(f);
    if (nread > (size_t)len) {
        nread = (size_t)len;
    }
    buf[nread] = '\0';
    userconfig_source_digest("present", buf, nread, source_sha256);

    yyjson_doc *doc = yyjson_read(buf, nread, 0);
    free(buf);

    if (!doc) {
        cbm_log_warn("userconfig.corrupt_json", "path", path);
        return 0; /* corrupt JSON — silently ignore (fail-open) */
    }

    yyjson_val *root = yyjson_doc_get_root(doc);
    int rc = parse_extra_extensions(root, entries, count, path);
    yyjson_doc_free(doc);
    return rc;
}

/* Retain the project input before parsing it; never reopen for declarations. */
static int load_project_source(const char *path, cbm_userconfig_t *cfg, cbm_userext_t **entries,
                               int *count) {
    cbm_userconfig_source_t *source = cfg->project_source;
    source->state = CBM_USERCONFIG_SOURCE_ERROR;
    userconfig_source_digest("read-error", NULL, 0, cfg->project_source_sha256);
    cbm_path_info_t info = {0};
    int status = cbm_path_info_utf8(path, &info);
    if (status == CBM_PATH_INFO_ABSENT) {
        source->state = CBM_USERCONFIG_SOURCE_ABSENT;
        /* Preserve the existing digest for actual absence. */
        userconfig_source_digest("missing-or-unreadable", NULL, 0, cfg->project_source_sha256);
        return 0;
    }
    if (status != CBM_PATH_INFO_OK || !info.is_regular || info.size > MAX_CONFIG_SIZE)
        return 0;
    char *bytes = cbm_arena_alloc(&source->arena, MAX_CONFIG_SIZE + 1U);
    if (!bytes)
        return CBM_NOT_FOUND;
    FILE *file = cbm_fopen(path, "rb");
    if (!file)
        return 0;
    size_t len = fread(bytes, 1, MAX_CONFIG_SIZE + 1U, file);
    bool valid = !ferror(file) && feof(file) && len <= MAX_CONFIG_SIZE;
    if (fclose(file) != 0)
        valid = false;
    if (!valid)
        return 0;
    bytes[len] = '\0';
    source->state = CBM_USERCONFIG_SOURCE_PRESENT;
    source->bytes = bytes;
    source->len = len;
    userconfig_source_digest(len ? "present" : "empty", bytes, len, cfg->project_source_sha256);
    if (!len)
        return 0;
    yyjson_read_err error = {0};
    yyjson_doc *doc = yyjson_read_opts(bytes, len, 0, NULL, &error);
    if (!doc) {
        if (error.code == YYJSON_READ_ERROR_MEMORY_ALLOCATION)
            return CBM_NOT_FOUND;
        cbm_log_warn("userconfig.corrupt_json", "path", path);
        return 0;
    }
    int rc = parse_extra_extensions(yyjson_doc_get_root(doc), entries, count, path);
    yyjson_doc_free(doc);
    return rc;
}

/* ── Public API ──────────────────────────────────────────────────── */

static cbm_userconfig_t *userconfig_load(const char *repo_path, bool retain_source) {
    /* The retained source first: a failure here has nothing else to undo. */
    cbm_userconfig_source_t *source = NULL;
    if (retain_source) {
        CBMArena arena;
        cbm_arena_init(&arena);
        source = cbm_arena_calloc(&arena, sizeof(*source));
        if (!source) {
            cbm_arena_destroy(&arena);
            return NULL;
        }
        source->arena = arena;
        source->state = CBM_USERCONFIG_SOURCE_ABSENT;
    }
    cbm_userconfig_t *cfg = calloc(CBM_ALLOC_ONE, sizeof(cbm_userconfig_t));
    if (!cfg) {
        userconfig_source_free(source);
        return NULL;
    }
    cfg->project_source = source;

    cbm_userext_t *entries = NULL;
    int count = 0;

    /* ── Step 1: Load global config ── */
    enum { PATH_BUF_SZ = 1280 };
    const char *cfg_base = cbm_app_config_dir();
    const char *cfg_fallback = cfg_base ? cfg_base : "/tmp";
    char global_path[PATH_BUF_SZ];
    snprintf(global_path, sizeof(global_path), "%s/codebase-memory-mcp/config.json", cfg_fallback);

    if (load_config_file(global_path, &entries, &count, cfg->global_source_sha256) != 0) {
        for (int i = 0; i < count; i++) {
            free(entries[i].ext);
        }
        free(entries);
        userconfig_source_free(cfg->project_source);
        free(cfg);
        return NULL;
    }

    int global_count = count; /* entries[0..global_count) are from global */

    /* ── Step 2: Load project config ── */
    userconfig_source_digest("not-applicable", NULL, 0, cfg->project_source_sha256);
    if (repo_path && repo_path[0]) {
        char project_path[PATH_BUF_SZ];
        int path_len =
            snprintf(project_path, sizeof(project_path), "%s/.codebase-memory.json", repo_path);
        int rc;
        if (retain_source) {
            if (path_len < 0 || (size_t)path_len >= sizeof(project_path)) {
                cfg->project_source->state = CBM_USERCONFIG_SOURCE_ERROR;
                userconfig_source_digest("path-error", NULL, 0, cfg->project_source_sha256);
                rc = 0;
            } else {
                rc = load_project_source(project_path, cfg, &entries, &count);
            }
        } else {
            rc = load_config_file(project_path, &entries, &count, cfg->project_source_sha256);
        }
        if (rc != 0) {
            /* Free already-allocated entries */
            for (int i = 0; i < count; i++) {
                free(entries[i].ext);
            }
            free(entries);
            userconfig_source_free(cfg->project_source);
            free(cfg);
            return NULL;
        }
    }

    /*
     * ── Step 3: Dedup — project entries win over global ──
     *
     * For any extension that appears in both global (indices 0..global_count)
     * and project (indices global_count..count), remove the global entry by
     * replacing it with the last global entry (order-insensitive dedup).
     */
    for (int p = global_count; p < count; p++) {
        for (int g = 0; g < global_count; g++) {
            if (entries[g].ext && strcmp(entries[g].ext, entries[p].ext) == 0) {
                /* Remove global entry: overwrite with last global entry */
                free(entries[g].ext);
                entries[g] = entries[global_count - SKIP_ONE];
                entries[global_count - SKIP_ONE].ext = NULL; /* mark as consumed */
                global_count--;
                break;
            }
        }
    }

    /*
     * Compact: remove any NULL-ext slots left by the dedup step.
     * (Those are the consumed "last global" entries.)
     */
    int write_idx = 0;
    for (int i = 0; i < count; i++) {
        if (entries[i].ext != NULL) {
            entries[write_idx++] = entries[i];
        }
    }
    count = write_idx;

    cfg->entries = entries;
    cfg->count = count;
    return cfg;
}

cbm_userconfig_t *cbm_userconfig_load(const char *repo_path) {
    return userconfig_load(repo_path, false);
}

cbm_userconfig_t *cbm_userconfig_load_with_source(const char *repo_path) {
    return userconfig_load(repo_path, true);
}

cbm_userconfig_source_state_t cbm_userconfig_project_source(const cbm_userconfig_t *cfg,
                                                            const char **bytes, size_t *len) {
    if (bytes)
        *bytes = NULL;
    if (len)
        *len = 0;
    if (!cfg || !cfg->project_source)
        return CBM_USERCONFIG_SOURCE_ERROR;
    const cbm_userconfig_source_t *source = cfg->project_source;
    if (source->state == CBM_USERCONFIG_SOURCE_PRESENT) {
        if (bytes)
            *bytes = source->bytes;
        if (len)
            *len = source->len;
    }
    return source->state;
}

CBMLanguage cbm_userconfig_lookup(const cbm_userconfig_t *cfg, const char *ext) {
    if (!cfg || !ext || !ext[0]) {
        return CBM_LANG_COUNT;
    }
    for (int i = 0; i < cfg->count; i++) {
        if (cfg->entries[i].ext && strcmp(cfg->entries[i].ext, ext) == 0) {
            return cfg->entries[i].lang;
        }
    }
    return CBM_LANG_COUNT;
}

void cbm_userconfig_free(cbm_userconfig_t *cfg) {
    if (!cfg) {
        return;
    }
    if (cfg->project_source && cfg->project_source->owns_config_arena) {
        CBMArena arena = cfg->project_source->arena;
        cbm_arena_destroy(&arena);
        return;
    }
    for (int i = 0; i < cfg->count; i++) {
        free(cfg->entries[i].ext);
    }
    free(cfg->entries);
    userconfig_source_free(cfg->project_source);
    free(cfg);
}

/* Frozen project-only input. The legacy pathname loaders remain fail-open. */
static bool snapshot_key_is(yyjson_val *key, const char *name) {
    size_t len = strlen(name);
    return yyjson_get_len(key) == len && memcmp(yyjson_get_str(key), name, len) == 0;
}

static cbm_userconfig_snapshot_status_t snapshot_extensions(yyjson_val *root, yyjson_val **out) {
    *out = NULL;
    if (!yyjson_is_obj(root))
        return CBM_USERCONFIG_SNAPSHOT_INVALID;
    yyjson_obj_iter iter;
    yyjson_obj_iter_init(root, &iter);
    yyjson_val *key;
    while ((key = yyjson_obj_iter_next(&iter)) != NULL) {
        if (!snapshot_key_is(key, "extra_extensions"))
            continue;
        if (*out)
            return CBM_USERCONFIG_SNAPSHOT_INVALID;
        *out = yyjson_obj_iter_get_val(key);
        if (!yyjson_is_obj(*out))
            return CBM_USERCONFIG_SNAPSHOT_INVALID;
    }
    return CBM_USERCONFIG_SNAPSHOT_OK;
}

static bool snapshot_string(yyjson_val *value) {
    if (!yyjson_is_str(value) || yyjson_get_len(value) == 0)
        return false;
    return memchr(yyjson_get_str(value), 0, yyjson_get_len(value)) == NULL;
}

/* Frozen config matching must not depend on the process locale. */
static CBMLanguage snapshot_language(yyjson_val *value) {
    const unsigned char *name = (const unsigned char *)yyjson_get_str(value);
    size_t len = yyjson_get_len(value);
    for (size_t i = 0; i < LANG_NAME_TABLE_SIZE; i++) {
        const char *alias = LANG_NAME_TABLE[i].name;
        if (strlen(alias) != len)
            continue;
        size_t j = 0;
        for (; j < len; j++) {
            unsigned char byte = name[j];
            if (byte >= 'A' && byte <= 'Z')
                byte = (unsigned char)(byte + ('a' - 'A'));
            if (byte != (unsigned char)alias[j])
                break;
        }
        if (j == len)
            return LANG_NAME_TABLE[i].lang;
    }
    return CBM_LANG_COUNT;
}

static cbm_userconfig_snapshot_status_t snapshot_entry(CBMArena *arena, cbm_userconfig_t *cfg,
                                                       yyjson_val *key, yyjson_val *value) {
    if (!snapshot_string(key) || !snapshot_string(value) || yyjson_get_str(key)[0] != '.')
        return CBM_USERCONFIG_SNAPSHOT_INVALID;
    const char *ext = yyjson_get_str(key);
    for (int i = 0; i < cfg->count; i++) {
        if (strcmp(cfg->entries[i].ext, ext) == 0)
            return CBM_USERCONFIG_SNAPSHOT_INVALID;
    }
    CBMLanguage language = snapshot_language(value);
    if (language == CBM_LANG_COUNT)
        return CBM_USERCONFIG_SNAPSHOT_UNSUPPORTED;
    char *copy = cbm_arena_strndup(arena, ext, yyjson_get_len(key));
    if (!copy)
        return CBM_USERCONFIG_SNAPSHOT_OOM;
    cfg->entries[cfg->count++] = (cbm_userext_t){.ext = copy, .lang = language};
    return CBM_USERCONFIG_SNAPSHOT_OK;
}

static cbm_userconfig_snapshot_status_t snapshot_parse(CBMArena *arena, cbm_userconfig_t *cfg) {
    cbm_userconfig_source_t *source = cfg->project_source;
    yyjson_read_err error = {0};
    yyjson_doc *doc = yyjson_read_opts((char *)source->bytes, source->len, 0, NULL, &error);
    if (!doc)
        return error.code == YYJSON_READ_ERROR_MEMORY_ALLOCATION ? CBM_USERCONFIG_SNAPSHOT_OOM
                                                                 : CBM_USERCONFIG_SNAPSHOT_INVALID;
    yyjson_val *extra = NULL;
    cbm_userconfig_snapshot_status_t status = snapshot_extensions(yyjson_doc_get_root(doc), &extra);
    size_t count = extra ? yyjson_obj_size(extra) : 0;
    /* The 64 KiB source cap already bounds count; keep allocation math explicit. */
    if (count > MAX_CONFIG_SIZE || count > SIZE_MAX / sizeof(*cfg->entries))
        status = CBM_USERCONFIG_SNAPSHOT_LIMIT;
    if (status == CBM_USERCONFIG_SNAPSHOT_OK && count > 0) {
        cfg->entries = cbm_arena_calloc(arena, count * sizeof(*cfg->entries));
        if (!cfg->entries)
            status = CBM_USERCONFIG_SNAPSHOT_OOM;
    }
    if (status == CBM_USERCONFIG_SNAPSHOT_OK && extra) {
        yyjson_obj_iter iter;
        yyjson_obj_iter_init(extra, &iter);
        yyjson_val *key;
        while ((key = yyjson_obj_iter_next(&iter)) != NULL) {
            status = snapshot_entry(arena, cfg, key, yyjson_obj_iter_get_val(key));
            if (status != CBM_USERCONFIG_SNAPSHOT_OK)
                break;
        }
    }
    yyjson_doc_free(doc);
    return status;
}

static cbm_userconfig_snapshot_status_t snapshot_input(cbm_userconfig_source_state_t state,
                                                       const void *bytes, size_t len) {
    if (state == CBM_USERCONFIG_SOURCE_ABSENT)
        return (!bytes && len == 0) ? CBM_USERCONFIG_SNAPSHOT_OK : CBM_USERCONFIG_SNAPSHOT_INVALID;
    if (state != CBM_USERCONFIG_SOURCE_PRESENT || !bytes || len == 0)
        return CBM_USERCONFIG_SNAPSHOT_INVALID;
    return len > MAX_CONFIG_SIZE ? CBM_USERCONFIG_SNAPSHOT_LIMIT : CBM_USERCONFIG_SNAPSHOT_OK;
}

cbm_userconfig_snapshot_status_t cbm_userconfig_from_project_bytes(
    cbm_userconfig_source_state_t state, const void *bytes, size_t len, cbm_userconfig_t **out) {
    if (!out)
        return CBM_USERCONFIG_SNAPSHOT_INVALID;
    *out = NULL;
    cbm_userconfig_snapshot_status_t status = snapshot_input(state, bytes, len);
    if (status != CBM_USERCONFIG_SNAPSHOT_OK)
        return status;
    CBMArena arena;
    cbm_arena_init_lazy(&arena, CBM_ARENA_DEFAULT_BLOCK_SIZE);
    cbm_userconfig_t *cfg = cbm_arena_calloc(&arena, sizeof(*cfg));
    cbm_userconfig_source_t *source = cbm_arena_calloc(&arena, sizeof(*source));
    if (!cfg || !source) {
        cbm_arena_destroy(&arena);
        return CBM_USERCONFIG_SNAPSHOT_OOM;
    }
    cfg->project_source = source;
    source->state = state;
    source->len = len;
    if (state == CBM_USERCONFIG_SOURCE_PRESENT) {
        source->bytes = cbm_arena_strndup(&arena, bytes, len);
        status = source->bytes ? snapshot_parse(&arena, cfg) : CBM_USERCONFIG_SNAPSHOT_OOM;
    }
    if (status != CBM_USERCONFIG_SNAPSHOT_OK) {
        cbm_arena_destroy(&arena);
        return status;
    }
    userconfig_source_digest("disabled-frozen-v1", NULL, 0, cfg->global_source_sha256);
    userconfig_source_digest(state == CBM_USERCONFIG_SOURCE_ABSENT ? "missing-or-unreadable"
                                                                   : "present",
                             source->bytes, len, cfg->project_source_sha256);
    source->owns_config_arena = true;
    source->arena = arena;
    *out = cfg;
    return CBM_USERCONFIG_SNAPSHOT_OK;
}
