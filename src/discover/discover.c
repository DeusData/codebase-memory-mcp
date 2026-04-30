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
    /* Misc */
    ".qdrant_code_embeddings", ".tmp", "vendor", NULL};

static const char *FAST_SKIP_DIRS[] = {
    "generated", "gen",           "auto-generated", "fixtures",     "testdata",    "test_data",
    "__tests__", "__mocks__",     "__snapshots__",  "__fixtures__", "__test__",    "docs",
    "doc",       "documentation", "examples",       "example",      "samples",     "sample",
    "assets",    "static",        "public",         "media",        "third_party", "thirdparty",
    "3rdparty",  "external",      "migrations",     "seeds",        "e2e",         "integration",
    "locale",    "locales",       "i18n",           "l10n",         "scripts",     "tools",
    "hack",      "bin",           "build",          "out",          NULL};

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

/* ── Dynamic file list ───────────────────────────────────────────── */

typedef struct {
    cbm_file_info_t *files;
    int count;
    int capacity;
} file_list_t;

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

/* ── Recursive walk ──────────────────────────────────────────────── */

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
                                  const cbm_gitignore_t *cbmignore, const cbm_gitignore_t *local_gi,
                                  const char *local_gi_prefix) {
    if (cbm_should_skip_dir(entry_name, opts ? opts->mode : CBM_MODE_FULL)) {
        return true;
    }
    if (gitignore && cbm_gitignore_matches(gitignore, rel_path, true)) {
        return true;
    }
    if (local_gi) {
        const char *lrel = local_rel_path(rel_path, local_gi_prefix);
        if (cbm_gitignore_matches(local_gi, lrel, true)) {
            return true;
        }
    }
    if (cbmignore && cbm_gitignore_matches(cbmignore, rel_path, true)) {
        return true;
    }
    return false;
}

/* Check if a regular file should be skipped (filters + gitignore + size). */
static bool should_skip_file(const char *entry_name, const char *rel_path,
                             const cbm_discover_opts_t *opts, const cbm_gitignore_t *gitignore,
                             const cbm_gitignore_t *cbmignore, const cbm_gitignore_t *local_gi,
                             const char *local_gi_prefix, off_t file_size) {
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
    if (local_gi) {
        const char *lrel = local_rel_path(rel_path, local_gi_prefix);
        if (cbm_gitignore_matches(local_gi, lrel, false)) {
            return true;
        }
    }
    if (cbmignore && cbm_gitignore_matches(cbmignore, rel_path, false)) {
        return true;
    }
    if (opts && opts->max_file_size > 0 && file_size > opts->max_file_size) {
        return true;
    }
    return false;
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

/* Stat a path, skipping symlinks. Returns 0 on success, -1 to skip. */
static int safe_stat(const char *abs_path, struct stat *st) {
#ifdef _WIN32
    if (stat(abs_path, st) != 0) {
        return CBM_NOT_FOUND;
    }
#else
    if (lstat(abs_path, st) != 0) {
        return CBM_NOT_FOUND;
    }
    if (S_ISLNK(st->st_mode)) {
        return CBM_NOT_FOUND;
    }
#endif
    return 0;
}

/* Process a single regular file entry during directory walk. */
static void walk_dir_process_file(const char *abs_path, const char *rel_path, const char *name,
                                  const cbm_discover_opts_t *opts, const cbm_gitignore_t *gitignore,
                                  const cbm_gitignore_t *cbmignore, const cbm_gitignore_t *local_gi,
                                  const char *local_gi_prefix, off_t size, file_list_t *out) {
    if (should_skip_file(name, rel_path, opts, gitignore, cbmignore, local_gi, local_gi_prefix,
                         size)) {
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
    if (stat(gi_path, &gi_st) == 0 && S_ISREG(gi_st.st_mode)) {
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
        if (!should_skip_directory(entry->name, rel_path, opts, gitignore, cbmignore,
                                   frame->local_gi, frame->local_gi_prefix)) {
            walk_push_subdir(stack, top, abs_path, rel_path, frame);
        }
    } else if (S_ISREG(st.st_mode)) {
        walk_dir_process_file(abs_path, rel_path, entry->name, opts, gitignore, cbmignore,
                              frame->local_gi, frame->local_gi_prefix, st.st_size, out);
    }
}

enum { GI_OWNED_CAP = 64 };

static void walk_dir(const char *dir_path, const char *rel_prefix, const cbm_discover_opts_t *opts,
                     const cbm_gitignore_t *gitignore, const cbm_gitignore_t *cbmignore,
                     file_list_t *out) {
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
            walk_dir_process_entry(entry, &frame, opts, gitignore, cbmignore, stack, &top, out);
        }
        cbm_closedir(d);
    }
    for (int i = 0; i < owned_count; i++) {
        cbm_gitignore_free(owned_gis[i]);
    }
    free(stack);
}

/* ── Public API ──────────────────────────────────────────────────── */

int cbm_discover(const char *repo_path, const cbm_discover_opts_t *opts, cbm_file_info_t **out,
                 int *count) {
    if (!repo_path || !out || !count) {
        return CBM_NOT_FOUND;
    }

    *out = NULL;
    *count = 0;

    /* Verify directory exists */
    struct stat st;
    if (stat(repo_path, &st) != 0 || !S_ISDIR(st.st_mode)) {
        return CBM_NOT_FOUND;
    }

    /* Load gitignore if .git directory exists */
    cbm_gitignore_t *gitignore = NULL;
    char gi_path[CBM_SZ_4K];
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
