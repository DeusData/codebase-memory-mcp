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

#include "foundation/compat_fs.h"
#include <stdint.h> // int64_t
#include <stdio.h>
#include <stdlib.h>
#include <string.h> // strdup
#include <sys/stat.h>

/* ── Hardcoded always-skip directories ───────────────────────────── */

static const char *ALWAYS_SKIP_DIRS[] = {
    /* VCS */
    ".git", ".hg", ".svn", ".worktrees",
    /* IDE */
    ".idea", ".vs", ".vscode", ".eclipse", ".claude",
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
    ".vercel", ".netlify",
    /* Vendored / third-party code (always skip — use CBM_MODE_DEP for dep source) */
    "vendor", "vendored", "third_party", "thirdparty", "3rdparty", "external",
    /* Misc */
    ".qdrant_code_embeddings", ".tmp", NULL};

/* Prefix patterns for vendored directory names that vary (e.g. "vendored_libs",
 * "vendor-bundle"). Checked when exact match fails. Kept short for performance. */
static const char *VENDORED_DIR_PREFIXES[] = {
    "vendor", "3rdparty", "third_party", "thirdparty", NULL};

static const char *FAST_SKIP_DIRS[] = {
    "generated", "gen",           "auto-generated", "fixtures",     "testdata",    "test_data",
    "__tests__", "__mocks__",     "__snapshots__",  "__fixtures__", "__test__",    "docs",
    "doc",       "documentation", "examples",       "example",      "samples",     "sample",
    "assets",    "static",        "public",         "media",        "migrations",  "seeds",
    "e2e",       "integration",   "locale",         "locales",      "i18n",        "l10n",
    "scripts",   "tools",         "hack",           "bin",          "build",       "out",
    NULL};

/* ── Ignored suffixes ────────────────────────────────────────────── */

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

/* ── Fast-mode skip filenames ────────────────────────────────────── */

static const char *FAST_SKIP_FILENAMES[] = {
    "LICENSE",        "LICENSE.txt",     "LICENSE.md",   "LICENSE-MIT",   "LICENSE-APACHE",
    "LICENCE",        "LICENCE.txt",     "LICENCE.md",   "CHANGELOG",     "CHANGELOG.md",
    "CHANGES.md",     "HISTORY",         "HISTORY.md",   "AUTHORS",       "AUTHORS.md",
    "CONTRIBUTORS",   "CONTRIBUTORS.md", "CODEOWNERS",   "go.sum",        "yarn.lock",
    "pnpm-lock.yaml", "Pipfile.lock",    "poetry.lock",  "Gemfile.lock",  "Cargo.lock",
    "mix.lock",       "flake.lock",      "pubspec.lock", "composer.lock", "package-lock.json",
    "configure",      "Makefile.in",     "config.guess", "config.sub",    NULL};

/* ── Fast-mode substring patterns ────────────────────────────────── */

static const char *FAST_PATTERNS[] = {".d.ts",      ".bundle.", ".chunk.", ".generated.",
                                      ".pb.go",     "_pb2.py",  ".pb2.py", "_grpc.pb.go",
                                      "_string.go", "mock_",    "_mock.",  "_test_helpers.",
                                      ".stories.",  ".spec.",   ".test.",  NULL};

/* ── Ignored JSON filenames ──────────────────────────────────────── */

/* package.json and composer.json REMOVED — they contain dep declarations
 * needed by pass_configlink and dep auto-discovery. Tree-sitter JSON
 * grammar + extract_defs.c already handle them correctly. */
static const char *IGNORED_JSON_FILES[] = {
    "package-lock.json",  "tsconfig.json",
    "jsconfig.json",      "composer.lock",
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

/* ── Helper: check if string ends with suffix ────────────────────── */

static bool ends_with(const char *s, const char *suffix) {
    size_t slen = strlen(s);
    size_t sufflen = strlen(suffix);
    if (sufflen > slen) {
        return false;
    }
    return strcmp(s + slen - sufflen, suffix) == 0;
}

/* ── Helper: check if string contains substring ──────────────────── */

static bool str_contains(const char *s, const char *sub) {
    return strstr(s, sub) != NULL;
}

/* ── Public filter functions ─────────────────────────────────────── */

/* DEP mode: minimal skip list — only VCS, IDE, caches, test dirs.
 * Keeps vendor/, dist/, bin/, scripts/, third_party/ for dep source. */
static const char *DEP_SKIP_DIRS[] = {
    ".git", ".hg", ".svn",
    ".idea", ".vs", ".vscode",
    "__pycache__", ".mypy_cache", ".pytest_cache", ".ruff_cache",
    ".cache", "htmlcov", "coverage",
    "node_modules",
    ".next", ".nuxt", ".angular",
    "__tests__", "__mocks__", "__snapshots__",
    NULL
};

/* Check if dirname starts with any vendored prefix (e.g. "vendor-bundle",
 * "vendored_libs", "third_party_deps"). Catches naming variations that
 * exact match misses. */
static bool has_vendored_prefix(const char *dirname) {
    for (int i = 0; VENDORED_DIR_PREFIXES[i]; i++) {
        size_t plen = strlen(VENDORED_DIR_PREFIXES[i]);
        if (strncmp(dirname, VENDORED_DIR_PREFIXES[i], plen) == 0) {
            /* Match if dirname equals prefix or next char is a separator */
            char next = dirname[plen];
            if (next == '\0' || next == '-' || next == '_' || next == '.') {
                return true;
            }
        }
    }
    return false;
}

bool cbm_should_skip_dir(const char *dirname, cbm_index_mode_t mode) {
    if (!dirname) {
        return false;
    }

    if (mode == CBM_MODE_DEP) {
        return str_in_list(dirname, DEP_SKIP_DIRS);
    }

    if (str_in_list(dirname, ALWAYS_SKIP_DIRS)) {
        return true;
    }

    /* Prefix-based vendored detection catches variations like
     * "vendored_libs", "vendor-bundle", "third_party_deps" */
    if (has_vendored_prefix(dirname)) {
        return true;
    }

    if (mode == CBM_MODE_FAST) {
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

    if (mode == CBM_MODE_FAST || mode == CBM_MODE_DEP) {
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

    if (mode == CBM_MODE_FAST || mode == CBM_MODE_DEP) {
        if (str_in_list(filename, FAST_SKIP_FILENAMES)) {
            return true;
        }
    }

    return false;
}

/* DEP mode skip patterns: skip tests/mocks but NOT .d.ts (TS API surface) */
static const char *DEP_SKIP_PATTERNS[] = {
    ".spec.", ".test.", ".stories.",
    "mock_", "_mock.", "_test_helpers.",
    ".generated.", ".pb.go", "_pb2.py",
    NULL
};

bool cbm_matches_fast_pattern(const char *filename, cbm_index_mode_t mode) {
    if (!filename) {
        return false;
    }

    if (mode == CBM_MODE_DEP) {
        for (int i = 0; DEP_SKIP_PATTERNS[i]; i++) {
            if (str_contains(filename, DEP_SKIP_PATTERNS[i])) {
                return true;
            }
        }
        return false;
    }

    if (mode != CBM_MODE_FAST) {
        return false;
    }

    for (int i = 0; FAST_PATTERNS[i]; i++) {
        if (str_contains(filename, FAST_PATTERNS[i])) {
            return true;
        }
    }

    return false;
}

/* ── Dynamic file list ───────────────────────────────────────────── */

typedef struct {
    cbm_file_info_t *files;
    int count;
    int capacity;
} file_list_t;

// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
static void fl_add(file_list_t *fl, const char *abs_path, const char *rel_path, CBMLanguage lang,
                   int64_t size) {
    if (fl->count >= fl->capacity) {
        int new_cap = fl->capacity ? fl->capacity * 2 : 256;
        cbm_file_info_t *new_files = realloc(fl->files, new_cap * sizeof(cbm_file_info_t));
        if (!new_files) {
            return;
        }
        fl->files = new_files;
        fl->capacity = new_cap;
    }

    cbm_file_info_t *fi = &fl->files[fl->count++];
    // NOLINTNEXTLINE(misc-include-cleaner) — strdup provided by standard header
    fi->path = strdup(abs_path);
    fi->rel_path = strdup(rel_path);
    fi->language = lang;
    fi->size = size;
}

/* ── Recursive walk ──────────────────────────────────────────────── */

// NOLINTNEXTLINE(bugprone-easily-swappable-parameters,misc-no-recursion)
static void walk_dir(const char *dir_path, const char *rel_prefix, const cbm_discover_opts_t *opts,
                     const cbm_gitignore_t *gitignore, const cbm_gitignore_t *cbmignore,
                     file_list_t *out) {
    cbm_dir_t *d = cbm_opendir(dir_path);
    if (!d) {
        return;
    }

    cbm_dirent_t *entry;
    while ((entry = cbm_readdir(d)) != NULL) {

        /* Build full and relative paths */
        char abs_path[4096];
        char rel_path[4096];
        snprintf(abs_path, sizeof(abs_path), "%s/%s", dir_path, entry->name);
        if (rel_prefix[0]) {
            snprintf(rel_path, sizeof(rel_path), "%s/%s", rel_prefix, entry->name);
        } else {
            snprintf(rel_path, sizeof(rel_path), "%s", entry->name);
        }

        struct stat st;
#ifdef _WIN32
        if (stat(abs_path, &st) != 0) {
            continue;
        }
        /* Windows: no symlink detection via stat */
#else
        if (lstat(abs_path, &st) != 0) {
            continue;
        }
        /* Skip symlinks */
        if (S_ISLNK(st.st_mode)) {
            continue;
        }
#endif

        if (S_ISDIR(st.st_mode)) {
            /* Check hardcoded directory skip */
            if (cbm_should_skip_dir(entry->name, opts ? opts->mode : CBM_MODE_FULL)) {
                continue;
            }

            /* Check gitignore */
            if (gitignore && cbm_gitignore_matches(gitignore, rel_path, true)) {
                continue;
            }
            if (cbmignore && cbm_gitignore_matches(cbmignore, rel_path, true)) {
                continue;
            }

            /* Recurse */
            walk_dir(abs_path, rel_path, opts, gitignore, cbmignore, out);
        } else if (S_ISREG(st.st_mode)) {
            cbm_index_mode_t mode = opts ? opts->mode : CBM_MODE_FULL;

            /* Check suffix filter */
            if (cbm_has_ignored_suffix(entry->name, mode)) {
                continue;
            }

            /* Check fast-mode filename skip */
            if (cbm_should_skip_filename(entry->name, mode)) {
                continue;
            }

            /* Check fast-mode patterns */
            if (cbm_matches_fast_pattern(entry->name, mode)) {
                continue;
            }

            /* Check gitignore */
            if (gitignore && cbm_gitignore_matches(gitignore, rel_path, false)) {
                continue;
            }
            if (cbmignore && cbm_gitignore_matches(cbmignore, rel_path, false)) {
                continue;
            }

            /* Check max file size */
            if (opts && opts->max_file_size > 0 && st.st_size > opts->max_file_size) {
                continue;
            }

            /* Detect language */
            CBMLanguage lang = cbm_language_for_filename(entry->name);

            /* Handle .m disambiguation */
            if (lang == CBM_LANG_COUNT) {
                /* No language detected — skip */
                continue;
            }

            /* Special: .m files need content-based disambiguation */
            const char *dot = strrchr(entry->name, '.');
            if (dot && strcmp(dot, ".m") == 0) {
                lang = cbm_disambiguate_m(abs_path);
            }

            /* Check ignored JSON files */
            if (lang == CBM_LANG_JSON && str_in_list(entry->name, IGNORED_JSON_FILES)) {
                continue;
            }

            fl_add(out, abs_path, rel_path, lang, st.st_size);
        }
    }

    cbm_closedir(d);
}

/* ── Public API ──────────────────────────────────────────────────── */

int cbm_discover(const char *repo_path, const cbm_discover_opts_t *opts, cbm_file_info_t **out,
                 int *count) {
    if (!repo_path || !out || !count) {
        return -1;
    }

    *out = NULL;
    *count = 0;

    /* Verify directory exists */
    struct stat st;
    if (stat(repo_path, &st) != 0 || !S_ISDIR(st.st_mode)) {
        return -1;
    }

    /* Load gitignore if .git directory exists */
    cbm_gitignore_t *gitignore = NULL;
    char gi_path[4096];
    snprintf(gi_path, sizeof(gi_path), "%s/.git", repo_path);
    struct stat gi_stat;
    if (stat(gi_path, &gi_stat) == 0 && S_ISDIR(gi_stat.st_mode)) {
        snprintf(gi_path, sizeof(gi_path), "%s/.gitignore", repo_path);
        gitignore = cbm_gitignore_load(gi_path);
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
    walk_dir(repo_path, "", opts, gitignore, cbmignore, &fl);

    /* Cleanup */
    cbm_gitignore_free(gitignore);
    cbm_gitignore_free(cbmignore);

    *out = fl.files;
    *count = fl.count;
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
