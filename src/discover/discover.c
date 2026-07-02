/*
 * discover.c — Recursive directory walk with filtering.
 *
 * Walks a repository directory tree, applying:
 *   1. Hardcoded directory skip patterns (60+ dirs like .git, node_modules)
 *   2. Hardcoded suffix filters (.pyc, .png, .wasm, etc.)
 *   3. Fast-mode additional filters (docs, examples, lock files, etc.)
 *   4. Gitignore-style pattern matching
 *   5. Language detection for accepted files
 */
#include "discover/discover.h"
#include "cbm.h" // CBMLanguage, CBM_LANG_COUNT, CBM_LANG_JSON

#include "foundation/constants.h"
#include "foundation/compat_fs.h"
#include "foundation/platform.h"
#ifdef _WIN32
#include "foundation/win_utf8.h"
#endif
#include <ctype.h>
#include <stdint.h> // int64_t
#include <stdio.h>
#include <stdlib.h>
#include <string.h> // strdup
#include <sys/stat.h>

int cbm_gitignore_match_result(const cbm_gitignore_t *gi, const char *rel_path, bool is_dir);

/* ── Hardcoded always-skip directories ──────────────────────────── */

static const char *ALWAYS_SKIP_DIRS[] = {
    /* VCS */
    ".git", ".hg", ".svn", ".worktrees",
    /* IDE */
    ".idea", ".vs", ".vscode", ".eclipse", ".claude", ".claude-worktrees", "Antigravity",
    /* Python */
    ".cache", ".eggs", ".env", ".mypy_cache", ".nox", ".pytest_cache", ".ruff_cache", ".tox",
    ".venv", "__pycache__", "env", "htmlcov", "site-packages", "venv",
    /* JS/TS */
    ".npm", ".nyc_output", ".pnpm-store", ".yarn", "bower_components", "coverage", "node_modules",
    ".next", ".nuxt", ".svelte-kit", ".angular", ".turbo", ".parcel-cache", ".docusaurus", ".expo",
    /* Build artifacts */
    "dist", "obj", "Pods", "target", "temp", "tmp", ".terraform", ".serverless", "bazel-bin",
    "bazel-out", "bazel-testlogs",
    /* Language caches */
    ".cargo", ".stack-work", ".dart_tool", "zig-cache", "zig-out", ".metals", ".bloop", ".bsp",
    ".ccls-cache", ".clangd", "elm-stuff", "_opam", ".cpcache", ".shadow-cljs",
    /* Deploy */
    ".vercel", ".netlify", "deploy", "deployed",
    /* Misc */
    ".qdrant_code_embeddings", ".tmp", "vendor", "vendored", NULL};

static const char *FAST_SKIP_DIRS[] = {
    "generated", "gen",           "auto-generated", "fixtures",     "testdata",    "test_data",
    "__tests__", "__mocks__",     "__snapshots__",  "__fixtures__", "__test__",    "docs",
    "doc",       "documentation", "examples",       "example",      "samples",     "sample",
    "assets",    "static",        "public",         "media",        "third_party", "thirdparty",
    "3rdparty",  "external",      "migrations",     "seeds",        "e2e",         "integration",
    "locale",    "locales",       "i18n",           "l10n",         "scripts",     "tools",
    "hack",      "bin",           "build",          "out",          NULL};

/* ── Ignored suffixes ───────────────────────────────── */

static const char *ALWAYS_IGNORED_SUFFIXES[] = {
    ".tmp",    "~",        ".pyc",  ".pyo",   ".o",   ".a",   ".so",  ".dll",
    ".class",  ".png",     ".jpg",  ".jpeg",  ".gif", ".ico", ".bmp", ".tiff",
    ".webp",   ".svg",     ".wasm", ".node",  ".exe", ".bin", ".dat", ".db",
    ".sqlite", ".sqlite3", ".woff", ".woff2", ".ttf", ".eot", ".otf", NULL};

static const char *FAST_IGNORED_SUFFIXES[] = {
    ".zip", ".tar",  ".gz",       ".bz2",  ".xz",  ".rar",    ".7z",      ".jar",
    ".war", ".ear",  ".mp3",      ".mp4",  ".avi", ".mov",    ".wav",     ".flac",
    ".ogg", ".mkv",  ".webm",     ".pdf",  ".doc", ".docx",   ".xls",     ".xlsx",
    ".ppt", ".pptx", ".odt",      ".ods",  ".map", ".min.js", ".min.css", ".pem",
    ".crt", ".key",  ".cer",      ".p12",  ".pb",  ".avro",   ".parquet", ".beam",
    ".elc", ".rlib", ".coverage", ".prof", ".out", ".patch",  ".diff",    NULL};

/* ── Fast-mode skip filenames ─────────────────────── */

static const char *FAST_SKIP_FILENAMES[] = {
    "LICENSE",        "LICENSE.txt",     "LICENSE.md",   "LICENSE-MIT",   "LICENSE-APACHE",
    "LICENCE",        "LICENCE.txt",     "LICENCE.md",   "CHANGELOG",     "CHANGELOG.md",
    "CHANGES.md",     "HISTORY",         "HISTORY.md",   "AUTHORS",       "AUTHORS.md",
    "CONTRIBUTORS",   "CONTRIBUTORS.md", "CODEOWNERS",   "go.sum",        "yarn.lock",
    "pnpm-lock.yaml", "Pipfile.lock",    "poetry.lock",  "Gemfile.lock",  "Cargo.lock",
    "mix.lock",       "flake.lock",      "pubspec.lock", "composer.lock", "package-lock.json",
    "configure",      "Makefile.in",     "config.guess", "config.sub",    NULL};

/* ── Fast-mode substring patterns ───────────────────── */

static const char *FAST_PATTERNS[] = {".d.ts",      ".bundle.", ".chunk.", ".generated.",
                                      ".pb.go",     "_pb2.py",  ".pb2.py", "_grpc.pb.go",
                                      "_string.go", "mock_",    "_mock.",  "_test_helpers.",
                                      ".stories.",  ".spec.",   ".test.",  NULL};

/* ── Ignored JSON filenames ──────────────────────── */

static const char *IGNORED_JSON_FILES[] = {
    "package.json",       "package-lock.json", "tsconfig.json",
    "jsconfig.json",      "composer.json",     "composer.lock",
    "yarn.lock",          "openapi.json",      "swagger.json",
    "jest.config.json",   ".eslintrc.json",    ".prettierrc.json",
    ".babelrc.json",      "tslint.json",       "angular.json",
    "firebase.json",      "renovate.json",     "lerna.json",
    "turbo.json",         ".stylelintrc.json", "pnpm-lock.json",
    "deno.json",          "biome.json",        "devcontainer.json",
    ".devcontainer.json", "launch.json",       "settings.json",
    "extensions.json",    "tasks.json",        NULL};

/* ── Helper: check if string is in NULL-terminated array ─────────── */

static bool str_in_list(const char *s, const char *const *list) {
    for (int i = 0; list[i]; i++) {
        if (strcmp(s, list[i]) == 0) {
            return true;
        }
    }
    return false;
}

/* ── Helper: check if string ends with suffix ────────────── */

static bool ends_with(const char *s, const char *suffix) {
    size_t slen = strlen(s);
    size_t sufflen = strlen(suffix);
    if (sufflen > slen) {
        return false;
    }
    return strcmp(s + slen - sufflen, suffix) == 0;
}

/* ── Helper: check if string contains substring ───────────── */

static bool str_contains(const char *s, const char *sub) {
    return strstr(s, sub) != NULL;
}

/* ── Git global excludes resolution ───────────────────────────── */

enum { GIT_TILDE_PREFIX_LEN = 2 }; /* "~/". */

static bool ascii_ieq(const char *a, const char *b) {
    while (*a && *b) {
        if (tolower((unsigned char)*a) != tolower((unsigned char)*b)) {
            return false;
        }
        a++;
        b++;
    }
    return *a == '\0' && *b == '\0';
}

static char *trim_ws(char *s) {
    while (*s && isspace((unsigned char)*s)) {
        s++;
    }
    char *end = s + strlen(s);
    while (end > s && isspace((unsigned char)end[-1])) {
        end--;
    }
    *end = '\0';
    return s;
}

static void strip_inline_comment(char *s) {
    bool in_quote = false;
    char quote = '\0';
    for (char *p = s; *p; p++) {
        if ((*p == '"' || *p == '\'') && (p == s || p[-1] != '\\')) {
            if (!in_quote) {
                in_quote = true;
                quote = *p;
            } else if (*p == quote) {
                in_quote = false;
            }
            continue;
        }
        if (!in_quote && (*p == '#' || *p == ';') && (p == s || isspace((unsigned char)p[-1]))) {
            *p = '\0';
            return;
        }
    }
}

static char *strip_matching_quotes(char *s) {
    size_t len = strlen(s);
    if (len >= CBM_QUOTE_PAIR && ((s[0] == '"' && s[len - SKIP_ONE] == '"') ||
                                  (s[0] == '\'' && s[len - SKIP_ONE] == '\''))) {
        s[len - SKIP_ONE] = '\0';
        return s + SKIP_ONE;
    }
    return s;
}

static bool has_trailing_sep(const char *path) {
    size_t len = strlen(path);
    return len > 0 && (path[len - SKIP_ONE] == '/' || path[len - SKIP_ONE] == '\\');
}

static void path_join(char *out, size_t out_sz, const char *base, const char *rel) {
    if (!out || out_sz == 0) {
        return;
    }
    if (!base || base[0] == '\0') {
        snprintf(out, out_sz, "%s", rel ? rel : "");
    } else if (!rel || rel[0] == '\0') {
        snprintf(out, out_sz, "%s", base);
    } else if (has_trailing_sep(base)) {
        snprintf(out, out_sz, "%s%s", base, rel);
    } else {
        snprintf(out, out_sz, "%s/%s", base, rel);
    }
    cbm_normalize_path_sep(out);
}

static bool expand_git_path(const char *path, char *out, size_t out_sz) {
    if (!path || !path[0] || !out || out_sz == 0) {
        return false;
    }
    char normalized[CBM_SZ_4K];
    snprintf(normalized, sizeof(normalized), "%s", path);
    cbm_normalize_path_sep(normalized);

    if (normalized[0] != '~') {
        snprintf(out, out_sz, "%s", normalized);
        cbm_normalize_path_sep(out);
        return out[0] != '\0';
    }

    if (normalized[1] != '\0' && normalized[1] != '/') {
        return false; /* ~user expansion is intentionally not supported. */
    }

    const char *home = cbm_get_home_dir();
    if (!home || home[0] == '\0') {
        return false;
    }
    if (normalized[1] == '\0') {
        snprintf(out, out_sz, "%s", home);
        cbm_normalize_path_sep(out);
    } else {
        path_join(out, out_sz, home, normalized + GIT_TILDE_PREFIX_LEN);
    }
    return out[0] != '\0';
}

static bool read_core_excludes_file(const char *config_path, char *out, size_t out_sz) {
    FILE *f = cbm_fopen(config_path, "r");
    if (!f) {
        return false;
    }

    bool in_core = false;
    bool found = false;
    char line[CBM_SZ_4K];
    while (fgets(line, sizeof(line), f)) {
        char *s = trim_ws(line);
        if (s[0] == '\0' || s[0] == '#' || s[0] == ';') {
            continue;
        }

        if (s[0] == '[') {
            char *end = strchr(s, ']');
            if (!end) {
                in_core = false;
                continue;
            }
            *end = '\0';
            in_core = ascii_ieq(trim_ws(s + SKIP_ONE), "core");
            continue;
        }

        if (!in_core) {
            continue;
        }

        char *eq = strchr(s, '=');
        if (!eq) {
            continue;
        }
        *eq = '\0';
        char *key = trim_ws(s);
        char *value = trim_ws(eq + SKIP_ONE);
        strip_inline_comment(value);
        value = strip_matching_quotes(trim_ws(value));

        if (ascii_ieq(key, "excludesfile") && value[0] != '\0' &&
            expand_git_path(value, out, out_sz)) {
            found = true;
        }
    }

    fclose(f);
    return found;
}

static bool resolve_xdg_git_config_dir(char *out, size_t out_sz) {
    char env[CBM_SZ_4K];
    if (cbm_safe_getenv("XDG_CONFIG_HOME", env, sizeof(env), NULL) && env[0] != '\0') {
        snprintf(out, out_sz, "%s", env);
        cbm_normalize_path_sep(out);
        return true;
    }

    const char *home = cbm_get_home_dir();
    if (!home || home[0] == '\0') {
        return false;
    }
    path_join(out, out_sz, home, ".config");
    return out[0] != '\0';
}

static bool resolve_global_excludes_path(char *out, size_t out_sz) {
    char config_path[CBM_SZ_4K];

    const char *home = cbm_get_home_dir();
    if (home && home[0] != '\0') {
        path_join(config_path, sizeof(config_path), home, ".gitconfig");
        if (read_core_excludes_file(config_path, out, out_sz)) {
            return true;
        }
    }

    char xdg_config[CBM_SZ_4K];
    if (resolve_xdg_git_config_dir(xdg_config, sizeof(xdg_config))) {
        path_join(config_path, sizeof(config_path), xdg_config, "git/config");
        if (read_core_excludes_file(config_path, out, out_sz)) {
            return true;
        }
        path_join(out, out_sz, xdg_config, "git/ignore");
        return out[0] != '\0';
    }

    return false;
}

/* ── Public filter functions ─────────────────────── */

bool cbm_should_skip_dir(const char *dirname, cbm_index_mode_t mode) {
    if (!dirname) {
        return false;
    }

    if (str_in_list(dirname, ALWAYS_SKIP_DIRS)) {
        return true;
    }

    /* Fast discovery applies to both MODERATE and FAST — only FULL keeps everything. */
    if (mode != CBM_MODE_FULL) {
        if (str_in_list(dirname, FAST_SKIP_DIRS)) {
            return true;
        }
    }

    return false;
}

bool cbm_has_ignored_suffix(const char *filename, cbm_index_mode_t mode) {
    if (!filename) {
        return false;
    }

    for (int i = 0; ALWAYS_IGNORED_SUFFIXES[i]; i++) {
        if (ends_with(filename, ALWAYS_IGNORED_SUFFIXES[i])) {
            return true;
        }
    }

    if (mode != CBM_MODE_FULL) {
        for (int i = 0; FAST_IGNORED_SUFFIXES[i]; i++) {
            if (ends_with(filename, FAST_IGNORED_SUFFIXES[i])) {
                return true;
            }
        }
    }

    return false;
}

bool cbm_should_skip_filename(const char *filename, cbm_index_mode_t mode) {
    if (!filename) {
        return false;
    }

    if (mode != CBM_MODE_FULL) {
        if (str_in_list(filename, FAST_SKIP_FILENAMES)) {
            return true;
        }
    }

    return false;
}

bool cbm_matches_fast_pattern(const char *filename, cbm_index_mode_t mode) {
    if (!filename || mode == CBM_MODE_FULL) {
        return false;
    }

    for (int i = 0; FAST_PATTERNS[i]; i++) {
        if (str_contains(filename, FAST_PATTERNS[i])) {
            return true;
        }
    }

    return false;
}

/* ── Dynamic file list ────────────────────────── */

typedef struct {
    cbm_file_info_t *files;
    int count;
    int capacity;
    /* Directories skipped during the walk (rel paths), so callers can surface
     * which subtrees were dropped (#411). strdup'd; freed by the caller via
     * cbm_discover_free_excluded or internally when not requested. */
    char **excluded;
    int excluded_count;
    int excluded_cap;
} file_list_t;

static void file_list_add_excluded(file_list_t *fl, const char *rel_path) {
    if (!rel_path || rel_path[0] == '\0') {
        return;
    }
    if (fl->excluded_count >= fl->excluded_cap) {
        int new_cap = fl->excluded_cap ? fl->excluded_cap * PAIR_LEN : CBM_SZ_64;
        char **grown = realloc(fl->excluded, new_cap * sizeof(char *));
        if (!grown) {
            return;
        }
        fl->excluded = grown;
        fl->excluded_cap = new_cap;
    }
    char *copy = strdup(rel_path);
    if (!copy) {
        return;
    }
    fl->excluded[fl->excluded_count++] = copy;
}

static void fl_add(file_list_t *fl, const char *abs_path, const char *rel_path, CBMLanguage lang,
                   int64_t size) {
    if (fl->count >= fl->capacity) {
        int new_cap = fl->capacity ? fl->capacity * PAIR_LEN : CBM_SZ_256;
        cbm_file_info_t *new_files = realloc(fl->files, new_cap * sizeof(cbm_file_info_t));
        if (!new_files) {
            return;
        }
        fl->files = new_files;
        fl->capacity = new_cap;
    }

    cbm_file_info_t *fi = &fl->files[fl->count++];
    fi->path = strdup(abs_path);
    fi->rel_path = strdup(rel_path);
    fi->language = lang;
    fi->size = size;
}

/* ── Recursive walk ─────────────────────────────── */

/* Compute path relative to a nested .gitignore's directory.
 * "webapp/src/foo.js" with prefix "webapp" → "src/foo.js". */
static const char *local_rel_path(const char *rel_path, const char *local_prefix) {
    if (!local_prefix || local_prefix[0] == '\0') {
        return rel_path;
    }
    size_t prefix_len = strlen(local_prefix);
    if (strncmp(rel_path, local_prefix, prefix_len) == 0 && rel_path[prefix_len] == '/') {
        return rel_path + prefix_len + SKIP_ONE;
    }
    return rel_path;
}

/* Check if a directory entry should be skipped (hardcoded dirs + gitignore). */
static bool should_skip_directory(const char *entry_name, const char *rel_path,
                                  const cbm_discover_opts_t *opts, const cbm_gitignore_t *gitignore,
                                  const cbm_gitignore_t *global_gi,
                                  const cbm_gitignore_t *cbmignore, const cbm_gitignore_t *local_gi,
                                  const char *local_gi_prefix) {
    if (cbm_should_skip_dir(entry_name, opts ? opts->mode : CBM_MODE_FULL)) {
        return true;
    }
    if (gitignore && cbm_gitignore_matches(gitignore, rel_path, true)) {
        return true;
    }
    bool global_ignored = global_gi && cbm_gitignore_matches(global_gi, rel_path, true);
    if (local_gi) {
        const char *lrel = local_rel_path(rel_path, local_gi_prefix);
        if (cbm_gitignore_matches(local_gi, lrel, true)) {
            return true;
        }
    }
    if (cbmignore) {
        int cbm_result = cbm_gitignore_match_result(cbmignore, rel_path, true);
        if (cbm_result > 0) {
            return true;
        }
        if (cbm_result < 0 && global_ignored) {
            return false;
        }
    }
    return global_ignored;
}

/* Check if a regular file should be skipped (filters + gitignore + size). */
static bool should_skip_file(const char *entry_name, const char *rel_path,
                             const cbm_discover_opts_t *opts, const cbm_gitignore_t *gitignore,
                             const cbm_gitignore_t *global_gi, const cbm_gitignore_t *cbmignore,
                             const cbm_gitignore_t *local_gi, const char *local_gi_prefix,
                             off_t file_size) {
    cbm_index_mode_t mode = opts ? opts->mode : CBM_MODE_FULL;
    if (cbm_has_ignored_suffix(entry_name, mode)) {
        return true;
    }
    if (cbm_should_skip_filename(entry_name, mode)) {
        return true;
    }
    if (cbm_matches_fast_pattern(entry_name, mode)) {
        return true;
    }
    if (gitignore && cbm_gitignore_matches(gitignore, rel_path, false)) {
        return true;
    }
    bool global_ignored = global_gi && cbm_gitignore_matches(global_gi, rel_path, false);
    if (local_gi) {
        const char *lrel = local_rel_path(rel_path, local_gi_prefix);
        if (cbm_gitignore_matches(local_gi, lrel, false)) {
            return true;
        }
    }
    if (cbmignore) {
        int cbm_result = cbm_gitignore_match_result(cbmignore, rel_path, false);
        if (cbm_result > 0) {
            return true;
        }
        if (cbm_result < 0 && global_ignored) {
            global_ignored = false;
        }
    }
    if (opts && opts->max_file_size > 0 && file_size > opts->max_file_size) {
        return true;
    }
    return global_ignored;
}

/* Detect language for a file, handling .m disambiguation and JSON filtering. */
static CBMLanguage detect_file_language(const char *entry_name, const char *abs_path) {
    CBMLanguage lang = cbm_language_for_filename(entry_name);
    if (lang == CBM_LANG_COUNT) {
        return CBM_LANG_COUNT;
    }
    /* Special: .m files need content-based disambiguation */
    const char *dot = strrchr(entry_name, '.');
    if (dot && strcmp(dot, ".m") == 0) {
        lang = cbm_disambiguate_m(abs_path);
    }
    /* Check ignored JSON files */
    if (lang == CBM_LANG_JSON && str_in_list(entry_name, IGNORED_JSON_FILES)) {
        return CBM_LANG_COUNT;
    }
    return lang;
}

/* UTF-8-safe stat: wide API on Windows, regular stat on POSIX. */
static int wide_stat(const char *path, struct stat *st) {
#ifdef _WIN32
    wchar_t *wpath = cbm_utf8_to_wide(path);
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
    return stat(path, st);
#endif
}

/* Stat a path, skipping symlinks. Returns 0 on success, -1 to skip. */
static int safe_stat(const char *abs_path, struct stat *st) {
#ifdef _WIN32
    return wide_stat(abs_path, st);
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

/* Process a single regular file entry during directory walk. */
static void walk_dir_process_file(const char *abs_path, const char *rel_path, const char *name,
                                  const cbm_discover_opts_t *opts, const cbm_gitignore_t *gitignore,
                                  const cbm_gitignore_t *global_gi,
                                  const cbm_gitignore_t *cbmignore, const cbm_gitignore_t *local_gi,
                                  const char *local_gi_prefix, off_t size, file_list_t *out) {
    if (should_skip_file(name, rel_path, opts, gitignore, global_gi, cbmignore, local_gi,
                         local_gi_prefix, size)) {
        return;
    }
    CBMLanguage lang = detect_file_language(name, abs_path);
    if (lang == CBM_LANG_COUNT) {
        return;
    }
    fl_add(out, abs_path, rel_path, lang, size);
}

typedef struct {
    char dir[CBM_SZ_4K];
    char prefix[CBM_SZ_4K];
    cbm_gitignore_t *local_gi;       /* nested .gitignore for this subtree */
    char local_gi_prefix[CBM_SZ_4K]; /* rel_prefix when local_gi was loaded */
} walk_frame_t;
#define WALK_STACK_CAP 512
/* Build abs/rel paths and process one directory entry. */
/* Try to load a nested .gitignore from this directory. Returns owned pointer or NULL. */
static cbm_gitignore_t *try_load_nested_gitignore(const walk_frame_t *frame) {
    if (frame->local_gi || frame->prefix[0] == '\0') {
        return NULL;
    }
    char gi_path[CBM_SZ_4K];
    snprintf(gi_path, sizeof(gi_path), "%s/.gitignore", frame->dir);
    struct stat gi_st;
    if (wide_stat(gi_path, &gi_st) == 0 && S_ISREG(gi_st.st_mode)) {
        return cbm_gitignore_load(gi_path);
    }
    return NULL;
}

/* Push a subdirectory onto the walk stack, inheriting local gitignore context. */
static void walk_push_subdir(walk_frame_t *stack, int *top, const char *abs_path,
                             const char *rel_path, const walk_frame_t *parent) {
    if (*top >= WALK_STACK_CAP) {
        return;
    }
    snprintf(stack[*top].dir, CBM_SZ_4K, "%s", abs_path);
    snprintf(stack[*top].prefix, CBM_SZ_4K, "%s", rel_path);
    stack[*top].local_gi = parent->local_gi;
    snprintf(stack[*top].local_gi_prefix, CBM_SZ_4K, "%s", parent->local_gi_prefix);
    (*top)++;
}

static void walk_dir_process_entry(cbm_dirent_t *entry, const walk_frame_t *frame,
                                   const cbm_discover_opts_t *opts,
                                   const cbm_gitignore_t *gitignore,
                                   const cbm_gitignore_t *global_gi,
                                   const cbm_gitignore_t *cbmignore, walk_frame_t *stack, int *top,
                                   file_list_t *out) {
    char abs_path[CBM_SZ_4K];
    char rel_path[CBM_SZ_4K];
    snprintf(abs_path, sizeof(abs_path), "%s/%s", frame->dir, entry->name);
    if (frame->prefix[0] != '\0') {
        snprintf(rel_path, sizeof(rel_path), "%s/%s", frame->prefix, entry->name);
    } else {
        snprintf(rel_path, sizeof(rel_path), "%s", entry->name);
    }

    struct stat st;
    if (safe_stat(abs_path, &st) != 0) {
        return;
    }

    if (S_ISDIR(st.st_mode)) {
        if (!should_skip_directory(entry->name, rel_path, opts, gitignore, global_gi, cbmignore,
                                   frame->local_gi, frame->local_gi_prefix)) {
            walk_push_subdir(stack, top, abs_path, rel_path, frame);
        } else {
            /* Record the excluded subtree root so callers can report it (#411). */
            file_list_add_excluded(out, rel_path);
        }
    } else if (S_ISREG(st.st_mode)) {
        walk_dir_process_file(abs_path, rel_path, entry->name, opts, gitignore, global_gi,
                              cbmignore, frame->local_gi, frame->local_gi_prefix, st.st_size, out);
    }
}

enum { GI_OWNED_CAP = 64 };

static void walk_dir(const char *dir_path, const char *rel_prefix, const cbm_discover_opts_t *opts,
                     const cbm_gitignore_t *gitignore, const cbm_gitignore_t *global_gi,
                     const cbm_gitignore_t *cbmignore, file_list_t *out) {
    walk_frame_t *stack = calloc(WALK_STACK_CAP, sizeof(walk_frame_t));
    if (!stack) {
        return;
    }
    /* Collect all owned gitignores — freed at the end because child frames
     * on the stack hold borrowed pointers to them. */
    cbm_gitignore_t *owned_gis[GI_OWNED_CAP];
    int owned_count = 0;

    int top = 0;
    snprintf(stack[top].dir, CBM_SZ_4K, "%s", dir_path);
    snprintf(stack[top].prefix, CBM_SZ_4K, "%s", rel_prefix);
    top++;

    while (top > 0) {
        walk_frame_t frame = stack[--top];

        cbm_gitignore_t *loaded = try_load_nested_gitignore(&frame);
        if (loaded) {
            frame.local_gi = loaded;
            snprintf(frame.local_gi_prefix, sizeof(frame.local_gi_prefix), "%s", frame.prefix);
            if (owned_count < GI_OWNED_CAP) {
                owned_gis[owned_count++] = loaded;
            }
        }

        cbm_dir_t *d = cbm_opendir(frame.dir);
        if (!d) {
            continue;
        }

        cbm_dirent_t *entry;
        while ((entry = cbm_readdir(d)) != NULL) {
            walk_dir_process_entry(entry, &frame, opts, gitignore, global_gi, cbmignore, stack,
                                   &top, out);
        }
        cbm_closedir(d);
    }
    for (int i = 0; i < owned_count; i++) {
        cbm_gitignore_free(owned_gis[i]);
    }
    free(stack);
}

/* ── Public API ───────────────────────────────── */

static bool discover_path_is_absolute(const char *path) {
    if (!path || !path[0]) {
        return false;
    }
    if (path[0] == '/' || path[0] == '\\') {
        return true;
    }
#ifdef _WIN32
    return isalpha((unsigned char)path[0]) && path[1] == ':';
#else
    return false;
#endif
}

/* Resolve the shared "common" git directory for repo_path.
 * Handles three layouts:
 *   1. <repo>/.git is a directory    - ordinary repo; common_dir == <repo>/.git
 *   2. <repo>/.git is a regular file - linked worktree gitlink "gitdir: <path>";
 *      the common dir is read from <git_dir>/commondir (git stores info/exclude +
 *      config there, shared across worktrees). Falls back to git_dir when no
 *      commondir file exists.
 *   3. neither - not a git repo.
 * Returns true when a git dir was resolved. Fixes the worktree case where
 * .git/info/exclude and core.excludesfile were silently dropped because the old
 * check required .git to be a directory (issue #489 only covered ordinary repos). */
static bool resolve_git_common_dir(const char *repo_path, char *common_dir, size_t cd_sz) {
    char dot_git[CBM_SZ_4K];
    snprintf(dot_git, sizeof(dot_git), "%s/.git", repo_path);
    struct stat st;
    if (wide_stat(dot_git, &st) != 0) {
        return false;
    }
    if (S_ISDIR(st.st_mode)) {
        snprintf(common_dir, cd_sz, "%s", dot_git);
        cbm_normalize_path_sep(common_dir);
        return true;
    }
    if (!S_ISREG(st.st_mode)) {
        return false;
    }

    /* Linked worktree: parse "gitdir: <path>" from the gitlink file.
     * cbm_fopen (not raw fopen) so non-ASCII repo paths open on Windows. */
    FILE *f = cbm_fopen(dot_git, "r");
    if (!f) {
        return false;
    }
    char git_dir[CBM_SZ_4K];
    bool got_git_dir = false;
    char line[CBM_SZ_4K];
    while (fgets(line, sizeof(line), f)) {
        char *gs = trim_ws(line);
        if (strncmp(gs, "gitdir:", 7) == 0) {
            char *val = trim_ws(gs + 7);
            if (val[0] != '\0') {
                if (discover_path_is_absolute(val)) {
                    snprintf(git_dir, sizeof(git_dir), "%s", val);
                    cbm_normalize_path_sep(git_dir);
                } else {
                    path_join(git_dir, sizeof(git_dir), repo_path, val);
                }
                got_git_dir = true;
            }
            break;
        }
    }
    fclose(f);
    if (!got_git_dir) {
        return false;
    }

    /* The shared dir holding info/exclude + config is named in <git_dir>/commondir
     * (typically a relative path like "../.."). Absent in single-worktree gitdirs. */
    char commondir_path[CBM_SZ_4K];
    path_join(commondir_path, sizeof(commondir_path), git_dir, "commondir");
    FILE *cf = cbm_fopen(commondir_path, "r");
    if (cf) {
        char cbuf[CBM_SZ_4K];
        bool resolved = false;
        if (fgets(cbuf, sizeof(cbuf), cf)) {
            char *cs = trim_ws(cbuf);
            if (cs[0] != '\0') {
                if (discover_path_is_absolute(cs)) {
                    snprintf(common_dir, cd_sz, "%s", cs);
                    cbm_normalize_path_sep(common_dir);
                } else {
                    path_join(common_dir, cd_sz, git_dir, cs);
                }
                resolved = true;
            }
        }
        fclose(cf);
        if (resolved) {
            return true;
        }
    }

    snprintf(common_dir, cd_sz, "%s", git_dir);
    cbm_normalize_path_sep(common_dir);
    return true;
}

int cbm_discover(const char *repo_path, const cbm_discover_opts_t *opts, cbm_file_info_t **out,
                 int *count) {
    return cbm_discover_ex(repo_path, opts, out, count, NULL, NULL);
}

int cbm_discover_ex(const char *repo_path, const cbm_discover_opts_t *opts, cbm_file_info_t **out,
                    int *count, char ***excluded_out, int *excluded_count_out) {
    if (excluded_out) {
        *excluded_out = NULL;
    }
    if (excluded_count_out) {
        *excluded_count_out = 0;
    }
    if (!repo_path || !out || !count) {
        return CBM_NOT_FOUND;
    }

    *out = NULL;
    *count = 0;

    /* Verify directory exists */
    struct stat st;
    if (wide_stat(repo_path, &st) != 0 || !S_ISDIR(st.st_mode)) {
        return CBM_NOT_FOUND;
    }

    /* Load gitignore sources for ordinary repos AND linked worktrees.
     * Sources merged in order (later patterns win on conflict):
     *   1. <repo>/.gitignore     — committed exclusions
     *   2. <common>/info/exclude — per-clone exclusions, not committed
     * <common> is the git common dir, resolved via resolve_git_common_dir() so a
     * worktree (where .git is a gitlink file) reads the shared info/exclude/config
     * just like a normal checkout. Both are folded into a single matcher so all
     * downstream call paths remain unchanged. Fixes issue #489: OOM on repos whose
     * worktrees are excluded only via .git/info/exclude (e.g. Sandcastle). */
    cbm_gitignore_t *gitignore = NULL;
    char gi_path[CBM_SZ_4K];
    struct stat gi_stat;
    /* Resolve the git common dir, transparently following a worktree gitlink so the
     * .git/info/exclude and core.excludesfile sources are honoured inside linked
     * worktrees too (where .git is a file pointing at the shared dir, not a directory). */
    char git_common_dir[CBM_SZ_4K];
    bool is_git_repo = resolve_git_common_dir(repo_path, git_common_dir, sizeof(git_common_dir));
    bool has_git_config = false;
    /* Always honour the .gitignore at the indexed-directory root, even when the
     * directory is not a git repo root (e.g. indexing a sub-package directly).
     * Fixes issue #510: a root .gitignore was silently ignored without .git/. */
    snprintf(gi_path, sizeof(gi_path), "%s/.gitignore", repo_path);
    gitignore = cbm_gitignore_load(gi_path);
    if (is_git_repo) {
        path_join(gi_path, sizeof(gi_path), git_common_dir, "config");
        has_git_config = wide_stat(gi_path, &gi_stat) == 0 && S_ISREG(gi_stat.st_mode);

        char exc_path[CBM_SZ_4K];
        path_join(exc_path, sizeof(exc_path), git_common_dir, "info/exclude");
        cbm_gitignore_t *git_exclude = cbm_gitignore_load(exc_path);
        if (git_exclude) {
            if (!gitignore) {
                gitignore = git_exclude;
            } else {
                /* On allocation failure the merge is atomic (dst unchanged), so
                 * the .gitignore patterns still apply; the exclude patterns are
                 * simply skipped — same as if .git/info/exclude were absent. */
                (void)cbm_gitignore_merge(gitignore, git_exclude);
                cbm_gitignore_free(git_exclude);
            }
        }
    }

    cbm_gitignore_t *global_gi = NULL;
    if (has_git_config && resolve_global_excludes_path(gi_path, sizeof(gi_path))) {
        global_gi = cbm_gitignore_load(gi_path);
    }

    /* Load cbmignore if specified or exists at repo root */
    cbm_gitignore_t *cbmignore = NULL;
    if (opts && opts->ignore_file) {
        cbmignore = cbm_gitignore_load(opts->ignore_file);
    } else {
        snprintf(gi_path, sizeof(gi_path), "%s/.cbmignore", repo_path);
        cbmignore = cbm_gitignore_load(gi_path);
    }

    /* Walk */
    file_list_t fl = {0};
    walk_dir(repo_path, "", opts, gitignore, global_gi, cbmignore, &fl);

    /* Cleanup */
    cbm_gitignore_free(gitignore);
    cbm_gitignore_free(global_gi);
    cbm_gitignore_free(cbmignore);

    *out = fl.files;
    *count = fl.count;

    /* Hand the excluded-dir list to the caller, or free it if not requested. */
    if (excluded_out) {
        *excluded_out = fl.excluded;
        if (excluded_count_out) {
            *excluded_count_out = fl.excluded_count;
        }
    } else {
        cbm_discover_free_excluded(fl.excluded, fl.excluded_count);
    }
    return 0;
}

void cbm_discover_free(cbm_file_info_t *files, int count) {
    if (!files) {
        return;
    }
    for (int i = 0; i < count; i++) {
        free(files[i].path);
        free(files[i].rel_path);
    }
    free(files);
}

void cbm_discover_free_excluded(char **excluded, int count) {
    if (!excluded) {
        return;
    }
    for (int i = 0; i < count; i++) {
        free(excluded[i]);
    }
    free(excluded);
}
