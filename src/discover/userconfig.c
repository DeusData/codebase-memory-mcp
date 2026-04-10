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
#include "foundation/log.h"

#include <yyjson/yyjson.h>

#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ── Process-global user config pointer ──────────────────────────── */

static const cbm_userconfig_t *g_userconfig = NULL;

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
    char lower[64];
    size_t i;
    for (i = 0; i < sizeof(lower) - 1 && s[i]; i++) {
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

/*
 * Get the XDG config dir for codebase-memory-mcp.
 * Writes "<dir>/codebase-memory-mcp" into buf (up to bufsz bytes).
 * Uses $XDG_CONFIG_HOME if set, else ~/.config.
 */
static void cbm_app_config_dir(char *buf, size_t bufsz) {
    // NOLINT(concurrency-mt-unsafe) — called before worker threads
    const char *xdg = getenv("XDG_CONFIG_HOME");
    if (xdg && xdg[0]) {
        snprintf(buf, bufsz, "%s/codebase-memory-mcp", xdg);
    } else {
        const char *home = getenv("HOME"); // NOLINT(concurrency-mt-unsafe)
        if (!home || !home[0]) {
            home = "/tmp";
        }
        snprintf(buf, bufsz, "%s/.config/codebase-memory-mcp", home);
    }
}

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
        cbm_userext_t *tmp = realloc(*entries, (size_t)(*count + 1) * sizeof(cbm_userext_t));
        if (!tmp) {
            return -1;
        }
        *entries = tmp;

        char *ext_copy = strdup(ext_str);
        if (!ext_copy) {
            return -1;
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
static int load_config_file(const char *path, cbm_userext_t **entries, int *count) {
    FILE *f = fopen(path, "rb");
    if (!f) {
        return 0; /* file absent — silently ignore */
    }

    fseek(f, 0, SEEK_END);
    long len = ftell(f);
    fseek(f, 0, SEEK_SET);

    if (len <= 0 || len > 65536) {
        fclose(f);
        if (len > 65536) {
            cbm_log_warn("userconfig.file_too_large", "path", path);
        }
        return 0;
    }

    char *buf = malloc((size_t)len + 1);
    if (!buf) {
        fclose(f);
        return -1;
    }

    size_t nread = fread(buf, 1, (size_t)len, f);
    fclose(f);
    buf[nread] = '\0';

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

/* ── Public API ──────────────────────────────────────────────────── */

cbm_userconfig_t *cbm_userconfig_load(const char *repo_path) {
    cbm_userconfig_t *cfg = calloc(1, sizeof(cbm_userconfig_t));
    if (!cfg) {
        return NULL;
    }

    cbm_userext_t *entries = NULL;
    int count = 0;

    /* ── Step 1: Load global config ── */
    enum { PATH_BUF_SZ = 1280 };
    char global_dir[1024];
    cbm_app_config_dir(global_dir, sizeof(global_dir));

    char global_path[PATH_BUF_SZ];
    snprintf(global_path, sizeof(global_path), "%s/config.json", global_dir);

    if (load_config_file(global_path, &entries, &count) != 0) {
        for (int i = 0; i < count; i++) {
            free(entries[i].ext);
        }
        free(entries);
        free(cfg);
        return NULL;
    }

    int global_count = count; /* entries[0..global_count) are from global */

    /* ── Step 2: Load project config ── */
    if (repo_path && repo_path[0]) {
        char project_path[PATH_BUF_SZ];
        snprintf(project_path, sizeof(project_path), "%s/.codebase-memory.json", repo_path);

        if (load_config_file(project_path, &entries, &count) != 0) {
            /* Free already-allocated entries */
            for (int i = 0; i < count; i++) {
                free(entries[i].ext);
            }
            free(entries);
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
                entries[g] = entries[global_count - 1];
                entries[global_count - 1].ext = NULL; /* mark as consumed */
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
    for (int i = 0; i < cfg->count; i++) {
        free(cfg->entries[i].ext);
    }
    free(cfg->entries);
    free(cfg);
}
