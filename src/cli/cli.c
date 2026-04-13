/*
 * cli.c — CLI subcommand handlers for install, uninstall, update, version.
 *
 * Port of Go cmd/codebase-memory-mcp/ install/update logic.
 * All functions accept explicit paths for testability.
 */
#include "cli/cli.h"
#include "foundation/compat.h"
#include "foundation/platform.h"
#include "foundation/constants.h"

/* CLI buffer size constants. */
enum {
    CLI_BUF_1K = 1024,
    CLI_BUF_512 = 512,
    CLI_BUF_256 = 256,
    CLI_BUF_128 = 128,
    CLI_BUF_4K = 4096,
    CLI_BUF_16 = 16,
    CLI_BUF_8 = 8,
    CLI_BUF_24 = 24,
    CLI_SKIP_ONE = 1,
    CLI_PAIR_LEN = 2,
    CLI_OCTAL_PERM = 0755,
    CLI_JSON_INDENT = 3,
    CLI_MAX_SCAN = 10,
    CLI_ERR = -1,
    CLI_OK = 0,
    CLI_TRUE = 1,
    CLI_ELEM_SIZE = 1,    /* fread/fwrite element size */
    CLI_IDX_1 = 1,        /* array index 1 */
    CLI_IDX_2 = 2,        /* array index 2 */
    CLI_STRTOL_BASE = 10, /* decimal base for strtol */
    CLI_STRTOL_HEX = 16,  /* hex base for strtol */
    CLI_BUF_2K = 2048,
    CLI_BUF_8K = 8192,
    CLI_BUF_32 = 32,
    CLI_INDENT_24 = 24,
    CLI_FIELD_1040 = 1040,
    CLI_MB_10 = 10,
    BYTE_SHIFT = 8,    /* bits per byte for multi-byte reads */
    SQL_NUL_TERM = -1, /* sqlite3 length = -1 means NUL-terminated */
    SQL_PARAM_1 = 1,   /* sqlite3_bind parameter index 1 */
    SQL_PARAM_2 = 2,
    SEMVER_PARTS = 3, /* major.minor.patch */
    DB_EXT_LEN = 3,   /* strlen(".db") */
    MIN_ARGC_CMD = 3,
    /* minimum argc for subcommand with arg */ /* sqlite3_bind parameter index 2 */ /* 10 MB cap
                                                                                       factor */
    CLI_MB_FACTOR = CLI_BUF_1K * CLI_BUF_1K,
    NUM_RETRIES = 5,
    NUM_DIRS = 4,
    DECOMP_FACTOR = 10,
    GROWTH_FACTOR = 2,
    MIN_ARGC_GET = 2,
    AUTO_YES = 1,
    AUTO_NO = -1,
    VARIANT_A = 1,
    VARIANT_B = 2,
    OCTAL_BASE = 8,
};

/* String length helper for strncmp. */
#define SLEN(s) (sizeof(s) - SKIP_ONE)

// the correct standard headers are included below but clang-tidy doesn't map them.
#include <ctype.h>
#ifndef _WIN32
#include <signal.h>
#include <unistd.h>
#endif
#ifdef __APPLE__
#include <mach-o/dyld.h>
#endif
#include "foundation/compat_fs.h"

#ifndef CBM_VERSION
#define CBM_VERSION "dev"
#endif
#include <errno.h>  // EEXIST
#include <fcntl.h>  // open, O_WRONLY, O_CREAT, O_TRUNC
#include <stdint.h> // uintptr_t
#include <stdio.h>
#include <stdlib.h>
#include <string.h>   // strtok_r
#include <sys/stat.h> // mode_t, S_IXUSR
#include <zlib.h>     // MAX_WBITS

/* yyjson for JSON read-modify-write */
#include "yyjson/yyjson.h"

/* SQLITE_TRANSIENT equivalent as a typed function pointer (avoids int-to-ptr cast).
 * sqlite3.h defines SQLITE_TRANSIENT as ((sqlite3_destructor_type)-1).
 * We replicate the same bit pattern via memcpy to satisfy performance-no-int-to-ptr. */
static void (*cbm_sqlite_transient_fn(void))(void *) {
    uintptr_t bits = (uintptr_t)CLI_ERR;
    void (*fp)(void *) = NULL;
    memcpy(&fp, &bits, sizeof(fp));
    return fp;
}
#define cbm_sqlite_transient (cbm_sqlite_transient_fn())

/* ── Constants ────────────────────────────────────────────────── */

/* Directory permissions: rwxr-x--- */
#define DIR_PERMS 0750

/* Decompression buffer cap (500 MB) */
#define DECOMPRESS_MAX_BYTES ((size_t)500 * CLI_BUF_1K * CBM_SZ_1K)

/* Tar header field offsets */
#define TAR_NAME_LEN 101    /* filename field: bytes 0-99 + NUL */
#define TAR_SIZE_OFFSET 124 /* octal size field offset */
#define TAR_SIZE_LEN 13     /* octal size field: bytes 124-135 + NUL */
#define TAR_TYPE_OFFSET 156 /* type flag byte */
#define TAR_BINARY_NAME "codebase-memory-mcp"
#define TAR_BINARY_NAME_LEN 19
#define TAR_BLOCK_SIZE CBM_SZ_512 /* tar record alignment */
#define TAR_BLOCK_MASK 511        /* TAR_BLOCK_SIZE - 1 */

/* ── Version ──────────────────────────────────────────────────── */

static const char *cli_version = "dev";

void cbm_cli_set_version(const char *ver) {
    if (ver) {
        cli_version = ver;
    }
}

const char *cbm_cli_get_version(void) {
    return cli_version;
}

/* ── Version comparison ───────────────────────────────────────── */

/* Parse semver major.minor.patch into array. Returns number of parts parsed. */
static int parse_semver(const char *v, int out[SEMVER_PARTS]) {
    out[0] = out[CLI_IDX_1] = out[CLI_IDX_2] = 0;
    /* Skip v prefix */
    if (*v == 'v' || *v == 'V') {
        v++;
    }

    int count = 0;
    while (*v && count < SEMVER_PARTS) {
        if (*v == '-') {
            break; /* stop at pre-release suffix */
        }
        char *endptr;
        long val = strtol(v, &endptr, CLI_STRTOL_BASE);
        out[count++] = (int)val;
        if (*endptr == '.') {
            v = endptr + CLI_SKIP_ONE;
        } else {
            break;
        }
    }
    return count;
}

static bool has_prerelease(const char *v) {
    if (*v == 'v' || *v == 'V') {
        v++;
    }
    return strchr(v, '-') != NULL;
}

int cbm_compare_versions(const char *a, const char *b) {
    int pa[SEMVER_PARTS];
    int pb[SEMVER_PARTS];
    parse_semver(a, pa);
    parse_semver(b, pb);

    for (int i = 0; i < SEMVER_PARTS; i++) {
        if (pa[i] != pb[i]) {
            return pa[i] - pb[i];
        }
    }

    /* Same base version — non-dev beats dev */
    bool a_pre = has_prerelease(a);
    bool b_pre = has_prerelease(b);
    if (a_pre && !b_pre) {
        return CLI_ERR;
    }
    if (!a_pre && b_pre) {
        return CLI_TRUE;
    }
    return 0;
}

/* ── Shell RC detection ───────────────────────────────────────── */

const char *cbm_detect_shell_rc(const char *home_dir) {
    static char buf[CLI_BUF_512];
    if (!home_dir || !home_dir[0]) {
        return "";
    }

    char shell_buf[CLI_BUF_256];
    const char *shell = cbm_safe_getenv("SHELL", shell_buf, sizeof(shell_buf), "");
    if (!shell) {
        shell = "";
    }

    if (strstr(shell, "/zsh")) {
        snprintf(buf, sizeof(buf), "%s/.zshrc", home_dir);
        return buf;
    }
    if (strstr(shell, "/bash")) {
        /* Prefer .bashrc, fall back to .bash_profile */
        snprintf(buf, sizeof(buf), "%s/.bashrc", home_dir);
        struct stat st;
        if (stat(buf, &st) == 0) {
            return buf;
        }
        snprintf(buf, sizeof(buf), "%s/.bash_profile", home_dir);
        return buf;
    }
    if (strstr(shell, "/fish")) {
        snprintf(buf, sizeof(buf), "%s/.config/fish/config.fish", home_dir);
        return buf;
    }

    /* Default to .profile */
    snprintf(buf, sizeof(buf), "%s/.profile", home_dir);
    return buf;
}

/* ── CLI binary detection ─────────────────────────────────────── */

/* PATH delimiter: `;` on Windows, `:` on POSIX. */
#ifdef _WIN32
#define PATH_DELIM ";"
#else
#define PATH_DELIM ":"
#endif

/* Check if a path exists and is executable.
 * On Windows, stat() doesn't set S_IXUSR — just check existence. */
static bool is_executable(const char *path) {
    struct stat st;
#ifdef _WIN32
    return stat(path, &st) == 0;
#else
    return stat(path, &st) == 0 && (st.st_mode & S_IXUSR);
#endif
}

/* Search for an executable named `name` in the PATH environment variable.
 * Returns the full path in `out` (max out_sz) if found, else empty string. */
static bool find_in_path(const char *name, char *out, size_t out_sz) {
    char path_copy[CLI_BUF_4K];
    if (!cbm_safe_getenv("PATH", path_copy, sizeof(path_copy), NULL)) {
        return false;
    }
    char *saveptr;
    char *dir = strtok_r(path_copy, PATH_DELIM, &saveptr);
    while (dir) {
        snprintf(out, out_sz, "%s/%s", dir, name);
        if (is_executable(out)) {
            return true;
        }
        dir = strtok_r(NULL, PATH_DELIM, &saveptr);
    }
    return false;
}

const char *cbm_find_cli(const char *name, const char *home_dir) {
    static char buf[CLI_BUF_512];
    if (!name || !name[0]) {
        return "";
    }
    if (find_in_path(name, buf, sizeof(buf))) {
        return buf;
    }
    if (!home_dir || !home_dir[0]) {
        return "";
    }
    enum { NUM_PATHS = 5 };
    char paths[NUM_PATHS][CLI_BUF_512];
    snprintf(paths[0], sizeof(paths[0]), "/usr/local/bin/%s", name);
    snprintf(paths[1], sizeof(paths[1]), "%s/.npm/bin/%s", home_dir, name);
    snprintf(paths[2], sizeof(paths[2]), "%s/.local/bin/%s", home_dir, name);
    snprintf(paths[3], sizeof(paths[3]), "%s/.cargo/bin/%s", home_dir, name);
#ifdef __APPLE__
    snprintf(paths[4], sizeof(paths[4]), "/opt/homebrew/bin/%s", name);
#else
    paths[4][0] = '\0';
#endif
    for (int i = 0; i < NUM_RETRIES; i++) {
        if (paths[i][0] && is_executable(paths[i])) {
            snprintf(buf, sizeof(buf), "%s", paths[i]);
            return buf;
        }
    }
    return "";
}

/* ── File utilities ───────────────────────────────────────────── */

int cbm_copy_file(const char *src, const char *dst) {
    FILE *in = fopen(src, "rb");
    if (!in) {
        return CLI_ERR;
    }

    FILE *out = fopen(dst, "wb");
    if (!out) {
        (void)fclose(in);
        return CLI_ERR;
    }

    char buf[CLI_BUF_8K];
    int err = 0;
    while (!feof(in) && !ferror(in)) {
        size_t n = fread(buf, CLI_ELEM_SIZE, sizeof(buf), in);
        if (n == 0) {
            break;
        }
        if (fwrite(buf, CLI_ELEM_SIZE, n, out) != n) {
            err = CLI_TRUE;
            break;
        }
    }

    if (err || ferror(in)) {
        (void)fclose(in);
        (void)fclose(out);
        return CLI_ERR;
    }

    (void)fclose(in);
    int rc = fclose(out);
    return rc == 0 ? 0 : CLI_ERR;
}

/* Replace a binary file. Unlinks the old file first (handles read-only and
 * running binaries on Unix where unlink succeeds on open files). On all
 * platforms, the caller should tell the user to restart after update. */
int cbm_replace_binary(const char *path, const unsigned char *data, int len, int mode) {
    if (!path || !data || len <= 0) {
        return CLI_ERR;
    }

    /* Remove existing file if it exists. On Unix, unlink works even if the
     * binary is running (inode stays alive until the process exits). On Windows,
     * unlink fails on running .exe — rename it aside as fallback. */
    struct stat st_check;
    if (stat(path, &st_check) == 0) {
        /* File exists — remove or rename it */
        if (cbm_unlink(path) != 0) {
#ifdef _WIN32
            /* Windows: can't unlink running .exe — rename aside */
            char old_path[CLI_BUF_1K];
            snprintf(old_path, sizeof(old_path), "%s.old", path);
            (void)cbm_unlink(old_path);
            if (rename(path, old_path) != 0) {
                return CLI_ERR;
            }
#else
            return CLI_ERR;
#endif
        }
    }

#ifndef _WIN32
    int fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, (mode_t)mode);
    if (fd < 0) {
        return CLI_ERR;
    }
    FILE *f = fdopen(fd, "wb");
    if (!f) {
        close(fd);
        return CLI_ERR;
    }
#else
    (void)mode;
    FILE *f = fopen(path, "wb");
    if (!f) {
        return CLI_ERR;
    }
#endif

    size_t written = fwrite(data, CLI_ELEM_SIZE, (size_t)len, f);
    (void)fclose(f);
    return written == (size_t)len ? 0 : CLI_ERR;
}

/* ── Skill file content (embedded) ────────────────────────────── */

/* Consolidated from 4 separate skills into 1 with progressive disclosure.
 * This embedded version is the single source of truth for the CLI installer.
 * Based on PR #81 by @gdilla — factual corrections applied. */
static const char skill_content[] =
    "---\n"
    "name: codebase-memory\n"
    "description: Use the codebase knowledge graph for structural code queries. "
    "Triggers on: explore the codebase, understand the architecture, what functions exist, "
    "show me the structure, who calls this function, what does X call, trace the call chain, "
    "find callers of, show dependencies, impact analysis, dead code, unused functions, "
    "high fan-out, refactor candidates, code quality audit, graph query syntax, "
    "Cypher query examples, edge types, how to use search_graph.\n"
    "---\n"
    "\n"
    "# Codebase Memory — Knowledge Graph Tools\n"
    "\n"
    "Graph tools return precise structural results in ~500 tokens vs ~80K for grep.\n"
    "\n"
    "## Quick Decision Matrix\n"
    "\n"
    "| Question | Tool call |\n"
    "|----------|----------|\n"
    "| Who calls X? | `trace_path(direction=\"inbound\")` |\n"
    "| What does X call? | `trace_path(direction=\"outbound\")` |\n"
    "| Full call context | `trace_path(direction=\"both\")` |\n"
    "| Find by name pattern | `search_graph(name_pattern=\"...\")` |\n"
    "| Dead code | `search_graph(max_degree=0, exclude_entry_points=true)` |\n"
    "| Cross-service edges | `query_graph` with Cypher |\n"
    "| Impact of local changes | `detect_changes()` |\n"
    "| Risk-classified trace | `trace_path(risk_labels=true)` |\n"
    "| Text search | `search_code` or Grep |\n"
    "\n"
    "## Exploration Workflow\n"
    "1. `list_projects` — check if project is indexed\n"
    "2. `get_graph_schema` — understand node/edge types\n"
    "3. `search_graph(label=\"Function\", name_pattern=\".*Pattern.*\")` — find code\n"
    "4. `get_code_snippet(qualified_name=\"project.path.FuncName\")` — read source\n"
    "\n"
    "## Tracing Workflow\n"
    "1. `search_graph(name_pattern=\".*FuncName.*\")` — discover exact name\n"
    "2. `trace_path(function_name=\"FuncName\", direction=\"both\", depth=3)` — trace\n"
    "3. `detect_changes()` — map git diff to affected symbols\n"
    "\n"
    "## Quality Analysis\n"
    "- Dead code: `search_graph(max_degree=0, exclude_entry_points=true)`\n"
    "- High fan-out: `search_graph(min_degree=10, relationship=\"CALLS\", "
    "direction=\"outbound\")`\n"
    "- High fan-in: `search_graph(min_degree=10, relationship=\"CALLS\", "
    "direction=\"inbound\")`\n"
    "\n"
    "## 14 MCP Tools\n"
    "`index_repository`, `index_status`, `list_projects`, `delete_project`,\n"
    "`search_graph`, `search_code`, `trace_path`, `detect_changes`,\n"
    "`query_graph`, `get_graph_schema`, `get_code_snippet`, `get_architecture`,\n"
    "`manage_adr`, `ingest_traces`\n"
    "\n"
    "## Edge Types\n"
    "CALLS, HTTP_CALLS, ASYNC_CALLS, IMPORTS, DEFINES, DEFINES_METHOD,\n"
    "HANDLES, IMPLEMENTS, OVERRIDE, USAGE, FILE_CHANGES_WITH,\n"
    "CONTAINS_FILE, CONTAINS_FOLDER, CONTAINS_PACKAGE\n"
    "\n"
    "## Cypher Examples (for query_graph)\n"
    "```\n"
    "MATCH (a)-[r:HTTP_CALLS]->(b) RETURN a.name, b.name, r.url_path, "
    "r.confidence LIMIT 20\n"
    "MATCH (f:Function) WHERE f.name =~ '.*Handler.*' RETURN f.name, f.file_path\n"
    "MATCH (a)-[r:CALLS]->(b) WHERE a.name = 'main' RETURN b.name\n"
    "```\n"
    "\n"
    "## Gotchas\n"
    "1. `search_graph(relationship=\"HTTP_CALLS\")` filters nodes by degree — "
    "use `query_graph` with Cypher to see actual edges.\n"
    "2. `query_graph` has a 200-row cap — use `search_graph` with degree filters "
    "for counting.\n"
    "3. `trace_path` needs exact names — use `search_graph(name_pattern=...)` first.\n"
    "4. `direction=\"outbound\"` misses cross-service callers — use "
    "`direction=\"both\"`.\n"
    "5. Results default to 10 per page — check `has_more` and use `offset`.\n";

static const char codex_instructions_content[] =
    "# Codebase Knowledge Graph\n"
    "\n"
    "This project uses codebase-memory-mcp to maintain a knowledge graph of the codebase.\n"
    "Use the MCP tools to explore and understand the code:\n"
    "\n"
    "- `search_graph` — find functions, classes, routes by pattern\n"
    "- `trace_path` — trace who calls a function or what it calls\n"
    "- `get_code_snippet` — read function source code\n"
    "- `query_graph` — run Cypher queries for complex patterns\n"
    "- `get_architecture` — high-level project summary\n"
    "\n"
    "Always prefer graph tools over grep for code discovery.\n";

/* Old skill names — cleaned up during install to remove stale directories. */
static const char *old_skill_names[] = {
    "codebase-memory-exploring",
    "codebase-memory-tracing",
    "codebase-memory-quality",
    "codebase-memory-reference",
};
enum { OLD_SKILL_COUNT = 4 };

static const cbm_skill_t skills[CBM_SKILL_COUNT] = {
    {"codebase-memory", skill_content},
};

const cbm_skill_t *cbm_get_skills(void) {
    return skills;
}

const char *cbm_get_codex_instructions(void) {
    return codex_instructions_content;
}

/* ── Recursive mkdir (via compat_fs) ──────────────────────────── */

static int mkdirp(const char *path, int mode) {
    return (int)cbm_mkdir_p(path, mode) ? 0 : CLI_ERR;
}

/* ── Recursive rmdir ──────────────────────────────────────────── */

enum { RMDIR_STACK_CAP = CBM_SZ_256 };

/* Scan one directory: push subdirs onto stack, unlink files. */
static void rmdir_scan_dir(const char *cur, char stack[][CLI_BUF_1K], int *top) {
    cbm_dir_t *d = cbm_opendir(cur);
    if (!d) {
        return;
    }
    cbm_dirent_t *ent;
    while ((ent = cbm_readdir(d)) != NULL) {
        char child[CLI_BUF_1K];
        snprintf(child, sizeof(child), "%s/%s", cur, ent->name);
        struct stat st;
        if (stat(child, &st) == 0 && S_ISDIR(st.st_mode)) {
            if (*top < RMDIR_STACK_CAP) {
                snprintf(stack[(*top)++], CLI_BUF_1K, "%s", child);
            }
        } else {
            cbm_unlink(child);
        }
    }
    cbm_closedir(d);
}

static int rmdir_recursive(const char *path) {
    char stack[RMDIR_STACK_CAP][CLI_BUF_1K];
    int top = 0;
    snprintf(stack[top++], CLI_BUF_1K, "%s", path);

    /* Post-order: collect all dirs depth-first, then rmdir in reverse. */
    char dirs[RMDIR_STACK_CAP][CLI_BUF_1K];
    int dir_count = 0;

    while (top > 0) {
        char *cur = stack[--top];
        if (dir_count < RMDIR_STACK_CAP) {
            snprintf(dirs[dir_count++], CLI_BUF_1K, "%s", cur);
        }
        rmdir_scan_dir(cur, stack, &top);
    }
    /* Remove dirs in reverse (deepest first). */
    int rc = 0;
    for (int i = dir_count - CLI_SKIP_ONE; i >= 0; i--) {
        if (cbm_rmdir(dirs[i]) != 0) {
            rc = CBM_NOT_FOUND;
        }
    }
    return rc;
}

/* ── Skill management ─────────────────────────────────────────── */

int cbm_install_skills(const char *skills_dir, bool force, bool dry_run) {
    if (!skills_dir) {
        return 0;
    }
    int count = 0;

    /* Clean up old 4-skill directories (consolidated into 1). */
    for (int i = 0; i < OLD_SKILL_COUNT; i++) {
        char old_path[CLI_BUF_1K];
        snprintf(old_path, sizeof(old_path), "%s/%s", skills_dir, old_skill_names[i]);
        struct stat st;
        if (stat(old_path, &st) == 0 && S_ISDIR(st.st_mode) && !dry_run) {
            rmdir_recursive(old_path);
        }
    }

    for (int i = 0; i < CBM_SKILL_COUNT; i++) {
        char skill_path[CLI_BUF_1K];
        snprintf(skill_path, sizeof(skill_path), "%s/%s", skills_dir, skills[i].name);
        char file_path[CLI_BUF_1K];
        snprintf(file_path, sizeof(file_path), "%s/SKILL.md", skill_path);

        /* Check if already exists */
        if (!force) {
            struct stat st;
            if (stat(file_path, &st) == 0) {
                continue;
            }
        }

        if (dry_run) {
            count++;
            continue;
        }

        if (mkdirp(skill_path, DIR_PERMS) != 0) {
            continue;
        }

        FILE *f = fopen(file_path, "w");
        if (!f) {
            continue;
        }
        (void)fwrite(skills[i].content, CLI_ELEM_SIZE, strlen(skills[i].content), f);
        (void)fclose(f);
        count++;
    }
    return count;
}

int cbm_remove_skills(const char *skills_dir, bool dry_run) {
    if (!skills_dir) {
        return 0;
    }
    int count = 0;

    for (int i = 0; i < CBM_SKILL_COUNT; i++) {
        char skill_path[CLI_BUF_1K];
        snprintf(skill_path, sizeof(skill_path), "%s/%s", skills_dir, skills[i].name);
        struct stat st;
        if (stat(skill_path, &st) != 0) {
            continue;
        }

        if (dry_run) {
            count++;
            continue;
        }

        if (rmdir_recursive(skill_path) == 0) {
            count++;
        }
    }
    return count;
}

bool cbm_remove_old_monolithic_skill(const char *skills_dir, bool dry_run) {
    if (!skills_dir) {
        return false;
    }

    char old_path[CLI_BUF_1K];
    snprintf(old_path, sizeof(old_path), "%s/codebase-memory-mcp", skills_dir);
    struct stat st;
    if (stat(old_path, &st) != 0 || !S_ISDIR(st.st_mode)) {
        return false;
    }

    if (dry_run) {
        return true;
    }
    return rmdir_recursive(old_path) == 0;
}

/* ── JSON config helpers (using yyjson) ───────────────────────── */

/* Read a JSON file into a yyjson document. Returns NULL on error. */
static yyjson_doc *read_json_file(const char *path) {
    FILE *f = fopen(path, "r");
    if (!f) {
        return NULL;
    }

    (void)fseek(f, 0, SEEK_END);
    long size = ftell(f);
    (void)fseek(f, 0, SEEK_SET);

    if (size <= 0 || size > (long)CLI_MB_10 * CLI_MB_FACTOR) {
        (void)fclose(f);
        return NULL;
    }

    char *buf = malloc((size_t)size + CLI_SKIP_ONE);
    if (!buf) {
        (void)fclose(f);
        return NULL;
    }

    size_t nread = fread(buf, CLI_ELEM_SIZE, (size_t)size, f);
    (void)fclose(f);
    if (nread > (size_t)size) {
        nread = (size_t)size;
    }
    buf[nread] = '\0';

    /* Allow JSONC (comments + trailing commas) — Zed settings.json uses this format */
    yyjson_read_flag flags = YYJSON_READ_ALLOW_COMMENTS | YYJSON_READ_ALLOW_TRAILING_COMMAS;
    yyjson_doc *doc = yyjson_read(buf, nread, flags);
    free(buf);
    return doc;
}

/* Write a mutable yyjson document to a file with pretty printing. */
static int write_json_file(const char *path, yyjson_mut_doc *doc) {
    /* Ensure parent directory exists */
    char dir[CLI_BUF_1K];
    snprintf(dir, sizeof(dir), "%s", path);
    char *last_slash = strrchr(dir, '/');
    if (last_slash) {
        *last_slash = '\0';
        mkdirp(dir, DIR_PERMS);
    }

    yyjson_write_flag flags = YYJSON_WRITE_PRETTY | YYJSON_WRITE_ESCAPE_UNICODE;
    size_t len;
    char *json = yyjson_mut_write(doc, flags, &len);
    if (!json) {
        return CLI_ERR;
    }

    FILE *f = fopen(path, "w");
    if (!f) {
        free(json);
        return CLI_ERR;
    }

    size_t written = fwrite(json, CLI_ELEM_SIZE, len, f);
    /* Add trailing newline */
    (void)fputc('\n', f);
    (void)fclose(f);
    free(json);

    return written == len ? 0 : CLI_ERR;
}

/* ── Editor MCP: Cursor/Windsurf/Gemini (mcpServers key) ──────── */

int cbm_install_editor_mcp(const char *binary_path, const char *config_path) {
    if (!binary_path || !config_path) {
        return CLI_ERR;
    }

    /* Read existing or start fresh */
    yyjson_mut_doc *mdoc = yyjson_mut_doc_new(NULL);
    if (!mdoc) {
        return CLI_ERR;
    }

    yyjson_doc *doc = read_json_file(config_path);
    yyjson_mut_val *root;
    if (doc) {
        root = yyjson_val_mut_copy(mdoc, yyjson_doc_get_root(doc));
        yyjson_doc_free(doc);
    } else {
        root = yyjson_mut_obj(mdoc);
    }
    if (!root) {
        yyjson_mut_doc_free(mdoc);
        return CLI_ERR;
    }
    yyjson_mut_doc_set_root(mdoc, root);

    /* Get or create mcpServers object */
    yyjson_mut_val *servers = yyjson_mut_obj_get(root, "mcpServers");
    if (!servers || !yyjson_mut_is_obj(servers)) {
        servers = yyjson_mut_obj(mdoc);
        yyjson_mut_obj_add_val(mdoc, root, "mcpServers", servers);
    }

    /* Remove existing entry if present */
    yyjson_mut_obj_remove_key(servers, "codebase-memory-mcp");

    /* Add our entry */
    yyjson_mut_val *entry = yyjson_mut_obj(mdoc);
    yyjson_mut_obj_add_str(mdoc, entry, "command", binary_path);
    yyjson_mut_obj_add_val(mdoc, servers, "codebase-memory-mcp", entry);

    int rc = write_json_file(config_path, mdoc);
    yyjson_mut_doc_free(mdoc);
    return rc;
}

int cbm_remove_editor_mcp(const char *config_path) {
    if (!config_path) {
        return CLI_ERR;
    }

    yyjson_doc *doc = read_json_file(config_path);
    if (!doc) {
        return CLI_ERR;
    }

    yyjson_mut_doc *mdoc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_val_mut_copy(mdoc, yyjson_doc_get_root(doc));
    yyjson_doc_free(doc);
    if (!root) {
        yyjson_mut_doc_free(mdoc);
        return CLI_ERR;
    }
    yyjson_mut_doc_set_root(mdoc, root);

    yyjson_mut_val *servers = yyjson_mut_obj_get(root, "mcpServers");
    if (!servers || !yyjson_mut_is_obj(servers)) {
        yyjson_mut_doc_free(mdoc);
        return 0;
    }

    yyjson_mut_obj_remove_key(servers, "codebase-memory-mcp");

    int rc = write_json_file(config_path, mdoc);
    yyjson_mut_doc_free(mdoc);
    return rc;
}

/* ── VS Code MCP (servers key with type:stdio) ────────────────── */

int cbm_install_vscode_mcp(const char *binary_path, const char *config_path) {
    if (!binary_path || !config_path) {
        return CLI_ERR;
    }

    yyjson_mut_doc *mdoc = yyjson_mut_doc_new(NULL);
    if (!mdoc) {
        return CLI_ERR;
    }

    yyjson_doc *doc = read_json_file(config_path);
    yyjson_mut_val *root;
    if (doc) {
        root = yyjson_val_mut_copy(mdoc, yyjson_doc_get_root(doc));
        yyjson_doc_free(doc);
    } else {
        root = yyjson_mut_obj(mdoc);
    }
    if (!root) {
        yyjson_mut_doc_free(mdoc);
        return CLI_ERR;
    }
    yyjson_mut_doc_set_root(mdoc, root);

    yyjson_mut_val *servers = yyjson_mut_obj_get(root, "servers");
    if (!servers || !yyjson_mut_is_obj(servers)) {
        servers = yyjson_mut_obj(mdoc);
        yyjson_mut_obj_add_val(mdoc, root, "servers", servers);
    }

    yyjson_mut_obj_remove_key(servers, "codebase-memory-mcp");

    yyjson_mut_val *entry = yyjson_mut_obj(mdoc);
    yyjson_mut_obj_add_str(mdoc, entry, "type", "stdio");
    yyjson_mut_obj_add_str(mdoc, entry, "command", binary_path);
    yyjson_mut_obj_add_val(mdoc, servers, "codebase-memory-mcp", entry);

    int rc = write_json_file(config_path, mdoc);
    yyjson_mut_doc_free(mdoc);
    return rc;
}

int cbm_remove_vscode_mcp(const char *config_path) {
    if (!config_path) {
        return CLI_ERR;
    }

    yyjson_doc *doc = read_json_file(config_path);
    if (!doc) {
        return CLI_ERR;
    }

    yyjson_mut_doc *mdoc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_val_mut_copy(mdoc, yyjson_doc_get_root(doc));
    yyjson_doc_free(doc);
    if (!root) {
        yyjson_mut_doc_free(mdoc);
        return CLI_ERR;
    }
    yyjson_mut_doc_set_root(mdoc, root);

    yyjson_mut_val *servers = yyjson_mut_obj_get(root, "servers");
    if (!servers || !yyjson_mut_is_obj(servers)) {
        yyjson_mut_doc_free(mdoc);
        return 0;
    }

    yyjson_mut_obj_remove_key(servers, "codebase-memory-mcp");

    int rc = write_json_file(config_path, mdoc);
    yyjson_mut_doc_free(mdoc);
    return rc;
}

/* ── Zed MCP (context_servers with command + args) ────────────── */

int cbm_install_zed_mcp(const char *binary_path, const char *config_path) {
    if (!binary_path || !config_path) {
        return CLI_ERR;
    }

    yyjson_mut_doc *mdoc = yyjson_mut_doc_new(NULL);
    if (!mdoc) {
        return CLI_ERR;
    }

    yyjson_doc *doc = read_json_file(config_path);
    yyjson_mut_val *root;
    if (doc) {
        root = yyjson_val_mut_copy(mdoc, yyjson_doc_get_root(doc));
        yyjson_doc_free(doc);
    } else {
        root = yyjson_mut_obj(mdoc);
    }
    if (!root) {
        yyjson_mut_doc_free(mdoc);
        return CLI_ERR;
    }
    yyjson_mut_doc_set_root(mdoc, root);

    yyjson_mut_val *servers = yyjson_mut_obj_get(root, "context_servers");
    if (!servers || !yyjson_mut_is_obj(servers)) {
        servers = yyjson_mut_obj(mdoc);
        yyjson_mut_obj_add_val(mdoc, root, "context_servers", servers);
    }

    yyjson_mut_obj_remove_key(servers, "codebase-memory-mcp");

    yyjson_mut_val *entry = yyjson_mut_obj(mdoc);
    yyjson_mut_obj_add_str(mdoc, entry, "command", binary_path);
    yyjson_mut_val *args = yyjson_mut_arr(mdoc);
    yyjson_mut_arr_add_str(mdoc, args, "");
    yyjson_mut_obj_add_val(mdoc, entry, "args", args);
    yyjson_mut_obj_add_val(mdoc, servers, "codebase-memory-mcp", entry);

    int rc = write_json_file(config_path, mdoc);
    yyjson_mut_doc_free(mdoc);
    return rc;
}

int cbm_remove_zed_mcp(const char *config_path) {
    if (!config_path) {
        return CLI_ERR;
    }

    yyjson_doc *doc = read_json_file(config_path);
    if (!doc) {
        return CLI_ERR;
    }

    yyjson_mut_doc *mdoc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_val_mut_copy(mdoc, yyjson_doc_get_root(doc));
    yyjson_doc_free(doc);
    if (!root) {
        yyjson_mut_doc_free(mdoc);
        return CLI_ERR;
    }
    yyjson_mut_doc_set_root(mdoc, root);

    yyjson_mut_val *servers = yyjson_mut_obj_get(root, "context_servers");
    if (!servers || !yyjson_mut_is_obj(servers)) {
        yyjson_mut_doc_free(mdoc);
        return 0;
    }

    yyjson_mut_obj_remove_key(servers, "codebase-memory-mcp");

    int rc = write_json_file(config_path, mdoc);
    yyjson_mut_doc_free(mdoc);
    return rc;
}

/* ── Agent detection ──────────────────────────────────────────── */

static bool dir_exists(const char *path) {
    struct stat st;
    return stat(path, &st) == 0 && S_ISDIR(st.st_mode);
}

cbm_detected_agents_t cbm_detect_agents(const char *home_dir) {
    cbm_detected_agents_t agents;
    memset(&agents, 0, sizeof(agents));
    if (!home_dir || !home_dir[0]) {
        return agents;
    }

    char path[CLI_BUF_1K];

    snprintf(path, sizeof(path), "%s/.claude", home_dir);
    agents.claude_code = dir_exists(path);

    snprintf(path, sizeof(path), "%s/.codex", home_dir);
    agents.codex = dir_exists(path);

    snprintf(path, sizeof(path), "%s/.gemini", home_dir);
    agents.gemini = dir_exists(path);

#ifdef __APPLE__
    snprintf(path, sizeof(path), "%s/Library/Application Support/Zed", home_dir);
#elif defined(_WIN32)
    snprintf(path, sizeof(path), "%s/AppData/Local/Zed", home_dir);
#else
    snprintf(path, sizeof(path), "%s/.config/zed", home_dir);
#endif
    agents.zed = dir_exists(path);

    agents.opencode = cbm_find_cli("opencode", home_dir)[0] != '\0';

    snprintf(path, sizeof(path), "%s/.gemini/antigravity", home_dir);
    if (dir_exists(path)) {
        agents.antigravity = true;
        agents.gemini = true;
    }

    agents.aider = cbm_find_cli("aider", home_dir)[0] != '\0';

#ifdef __APPLE__
    snprintf(path, sizeof(path),
             "%s/Library/Application Support/Code/User/globalStorage/kilocode.kilo-code", home_dir);
#elif defined(_WIN32)
    snprintf(path, sizeof(path), "%s/AppData/Roaming/Code/User/globalStorage/kilocode.kilo-code",
             home_dir);
#else
    snprintf(path, sizeof(path), "%s/.config/Code/User/globalStorage/kilocode.kilo-code", home_dir);
#endif
    agents.kilocode = dir_exists(path);

#ifdef __APPLE__
    snprintf(path, sizeof(path), "%s/Library/Application Support/Code/User", home_dir);
#elif defined(_WIN32)
    snprintf(path, sizeof(path), "%s/AppData/Roaming/Code/User", home_dir);
#else
    snprintf(path, sizeof(path), "%s/.config/Code/User", home_dir);
#endif
    agents.vscode = dir_exists(path);

    snprintf(path, sizeof(path), "%s/.openclaw", home_dir);
    agents.openclaw = dir_exists(path);

    return agents;
}

/* ── Shared agent instructions content ────────────────────────── */

static const char agent_instructions_content[] =
    "# Codebase Knowledge Graph (codebase-memory-mcp)\n"
    "\n"
    "This project uses codebase-memory-mcp to maintain a knowledge graph of the codebase.\n"
    "ALWAYS prefer MCP graph tools over grep/glob/file-search for code discovery.\n"
    "\n"
    "## Priority Order\n"
    "1. `search_graph` — find functions, classes, routes, variables by pattern\n"
    "2. `trace_path` — trace who calls a function or what it calls\n"
    "3. `get_code_snippet` — read specific function/class source code\n"
    "4. `query_graph` — run Cypher queries for complex patterns\n"
    "5. `get_architecture` — high-level project summary\n"
    "\n"
    "## When to fall back to grep/glob\n"
    "- Searching for string literals, error messages, config values\n"
    "- Searching non-code files (Dockerfiles, shell scripts, configs)\n"
    "- When MCP tools return insufficient results\n"
    "\n"
    "## Examples\n"
    "- Find a handler: `search_graph(name_pattern=\".*OrderHandler.*\")`\n"
    "- Who calls it: `trace_path(function_name=\"OrderHandler\", direction=\"inbound\")`\n"
    "- Read source: `get_code_snippet(qualified_name=\"pkg/orders.OrderHandler\")`\n";

const char *cbm_get_agent_instructions(void) {
    return agent_instructions_content;
}

/* ── Instructions file upsert ─────────────────────────────────── */

#define CMM_MARKER_START "<!-- codebase-memory-mcp:start -->"
#define CMM_MARKER_END "<!-- codebase-memory-mcp:end -->"

/* Read entire file into malloc'd buffer. Returns NULL on error. */
static char *read_file_str(const char *path, size_t *out_len) {
    FILE *f = fopen(path, "r");
    if (!f) {
        if (out_len) {
            *out_len = 0;
        }
        return NULL;
    }
    (void)fseek(f, 0, SEEK_END);
    long size = ftell(f);
    (void)fseek(f, 0, SEEK_SET);
    if (size < 0 || size > (long)CLI_MB_10 * CLI_MB_FACTOR) { /* cap at 10 MB */
        (void)fclose(f);
        return NULL;
    }

    char *buf = malloc((size_t)size + CLI_SKIP_ONE);
    if (!buf) {
        (void)fclose(f);
        return NULL;
    }
    size_t nread = fread(buf, CLI_ELEM_SIZE, (size_t)size, f);
    (void)fclose(f);
    if (nread > (size_t)size) {
        nread = (size_t)size;
    }
    buf[nread] = '\0';
    if (out_len) {
        *out_len = nread;
    }
    return buf;
}

/* Write string to file, creating parent dirs if needed. */
static int write_file_str(const char *path, const char *content) {
    /* Ensure parent directory */
    char dir[CLI_BUF_1K];
    snprintf(dir, sizeof(dir), "%s", path);
    char *last_slash = strrchr(dir, '/');
    if (last_slash) {
        *last_slash = '\0';
        mkdirp(dir, DIR_PERMS);
    }

    FILE *f = fopen(path, "w");
    if (!f) {
        return CLI_ERR;
    }
    size_t len = strlen(content);
    size_t written = fwrite(content, CLI_ELEM_SIZE, len, f);
    (void)fclose(f);
    return written == len ? 0 : CLI_ERR;
}

int cbm_upsert_instructions(const char *path, const char *content) {
    if (!path || !content) {
        return CLI_ERR;
    }

    size_t existing_len = 0;
    char *existing = read_file_str(path, &existing_len);

    /* Build the marker-wrapped section */
    size_t section_len = strlen(CMM_MARKER_START) + CLI_SKIP_ONE + strlen(content) +
                         strlen(CMM_MARKER_END) + CLI_SKIP_ONE;
    char *section = malloc(section_len + CLI_SKIP_ONE);
    if (!section) {
        free(existing);
        return CLI_ERR;
    }
    snprintf(section, section_len + SKIP_ONE, "%s\n%s%s\n", CMM_MARKER_START, content,
             CMM_MARKER_END);

    if (!existing) {
        /* File doesn't exist — create with just the section */
        int rc = write_file_str(path, section);
        free(section);
        return rc;
    }

    /* Check if markers already exist */
    char *start = strstr(existing, CMM_MARKER_START);
    char *end = start ? strstr(start, CMM_MARKER_END) : NULL;

    char *result;
    if (start && end) {
        /* Replace between markers (including markers themselves) */
        end += strlen(CMM_MARKER_END);
        /* Skip trailing newline after end marker */
        if (*end == '\n') {
            end++;
        }

        size_t prefix_len = (size_t)(start - existing);
        size_t suffix_len = strlen(end);
        size_t new_len = prefix_len + strlen(section) + suffix_len;
        result = malloc(new_len + CLI_SKIP_ONE);
        if (!result) {
            free(existing);
            free(section);
            return CLI_ERR;
        }
        memcpy(result, existing, prefix_len);
        memcpy(result + prefix_len, section, strlen(section));
        memcpy(result + prefix_len + strlen(section), end, suffix_len);
        result[new_len] = '\0';
    } else {
        /* Append section */
        size_t new_len = existing_len + CLI_SKIP_ONE + strlen(section);
        if (new_len > (size_t)CLI_MB_10 * CLI_MB_FACTOR) { /* 10 MB safety cap */
            free(existing);
            free(section);
            return CLI_ERR;
        }
        result = malloc(new_len + CLI_SKIP_ONE);
        if (!result) {
            free(existing);
            free(section);
            return CLI_ERR;
        }
        memcpy(result, existing, existing_len);
        result[existing_len] = '\n';
        memcpy(result + existing_len + SKIP_ONE, section, strlen(section));
        result[new_len] = '\0';
    }

    int rc = write_file_str(path, result);
    free(existing);
    free(section);
    free(result);
    return rc;
}

int cbm_remove_instructions(const char *path) {
    if (!path) {
        return CLI_ERR;
    }

    size_t len = 0;
    char *content = read_file_str(path, &len);
    if (!content) {
        return CLI_TRUE;
    }

    char *start = strstr(content, CMM_MARKER_START);
    char *end = start ? strstr(start, CMM_MARKER_END) : NULL;

    if (!start || !end) {
        free(content);
        return CLI_TRUE; /* not found */
    }

    end += strlen(CMM_MARKER_END);
    if (*end == '\n') {
        end++;
    }

    /* Also remove a leading newline before the start marker if present */
    if (start > content && *(start - CLI_SKIP_ONE) == '\n') {
        start--;
    }

    size_t prefix_len = (size_t)(start - content);
    size_t suffix_len = strlen(end);
    size_t new_len = prefix_len + suffix_len;
    char *result = malloc(new_len + CLI_SKIP_ONE);
    if (!result) {
        free(content);
        return CLI_ERR;
    }
    memcpy(result, content, prefix_len);
    memcpy(result + prefix_len, end, suffix_len);
    result[new_len] = '\0';

    int rc = write_file_str(path, result);
    free(content);
    free(result);
    return rc;
}

/* ── Codex MCP config (TOML) ─────────────────────────────────── */

#define CODEX_CMM_SECTION "[mcp_servers.codebase-memory-mcp]"

int cbm_upsert_codex_mcp(const char *binary_path, const char *config_path) {
    if (!binary_path || !config_path) {
        return CLI_ERR;
    }

    size_t len = 0;
    char *content = read_file_str(config_path, &len);

    /* Build our TOML section */
    char section[CLI_BUF_1K];
    snprintf(section, sizeof(section), "%s\ncommand = \"%s\"\n", CODEX_CMM_SECTION, binary_path);

    if (!content) {
        /* No file — create fresh */
        return write_file_str(config_path, section);
    }

    /* Check if our section already exists */
    char *existing = strstr(content, CODEX_CMM_SECTION);
    if (existing) {
        /* Remove old section: from [mcp_servers.codebase-memory-mcp] to next [section] or EOF */
        char *section_end = existing + strlen(CODEX_CMM_SECTION);
        /* Find next [section] header */
        char *next_section = strstr(section_end, "\n[");
        if (next_section) {
            next_section++; /* keep the newline before next section */
        }

        size_t prefix_len = (size_t)(existing - content);
        const char *suffix = next_section ? next_section : "";
        size_t suffix_len = strlen(suffix);
        size_t new_len = prefix_len + strlen(section) + CLI_SKIP_ONE + suffix_len;
        char *result = malloc(new_len + CLI_SKIP_ONE);
        if (!result) {
            free(content);
            return CLI_ERR;
        }
        memcpy(result, content, prefix_len);
        memcpy(result + prefix_len, section, strlen(section));
        result[prefix_len + strlen(section)] = '\n';
        memcpy(result + prefix_len + strlen(section) + CLI_SKIP_ONE, suffix, suffix_len);
        result[new_len] = '\0';

        int rc = write_file_str(config_path, result);
        free(content);
        free(result);
        return rc;
    }

    /* Append our section */
    size_t new_len = len + CLI_SKIP_ONE + strlen(section);
    char *result = malloc(new_len + CLI_SKIP_ONE);
    if (!result) {
        free(content);
        return CLI_ERR;
    }
    memcpy(result, content, len);
    result[len] = '\n';
    memcpy(result + len + SKIP_ONE, section, strlen(section));
    result[new_len] = '\0';

    int rc = write_file_str(config_path, result);
    free(content);
    free(result);
    return rc;
}

int cbm_remove_codex_mcp(const char *config_path) {
    if (!config_path) {
        return CLI_ERR;
    }

    size_t len = 0;
    char *content = read_file_str(config_path, &len);
    if (!content) {
        return CLI_TRUE;
    }

    char *existing = strstr(content, CODEX_CMM_SECTION);
    if (!existing) {
        free(content);
        return CLI_TRUE;
    }

    char *section_end = existing + strlen(CODEX_CMM_SECTION);
    char *next_section = strstr(section_end, "\n[");
    if (next_section) {
        next_section++;
    }

    /* Remove leading newline if present */
    if (existing > content && *(existing - CLI_SKIP_ONE) == '\n') {
        existing--;
    }

    size_t prefix_len = (size_t)(existing - content);
    const char *suffix = next_section ? next_section : "";
    size_t suffix_len = strlen(suffix);
    size_t new_len = prefix_len + suffix_len;
    char *result = malloc(new_len + CLI_SKIP_ONE);
    if (!result) {
        free(content);
        return CLI_ERR;
    }
    memcpy(result, content, prefix_len);
    memcpy(result + prefix_len, suffix, suffix_len);
    result[new_len] = '\0';

    int rc = write_file_str(config_path, result);
    free(content);
    free(result);
    return rc;
}

/* ── OpenCode MCP config (JSON with "mcp" key) ───────────────── */

int cbm_upsert_opencode_mcp(const char *binary_path, const char *config_path) {
    if (!binary_path || !config_path) {
        return CLI_ERR;
    }

    yyjson_mut_doc *mdoc = yyjson_mut_doc_new(NULL);
    if (!mdoc) {
        return CLI_ERR;
    }

    yyjson_doc *doc = read_json_file(config_path);
    yyjson_mut_val *root;
    if (doc) {
        root = yyjson_val_mut_copy(mdoc, yyjson_doc_get_root(doc));
        yyjson_doc_free(doc);
    } else {
        root = yyjson_mut_obj(mdoc);
    }
    if (!root) {
        yyjson_mut_doc_free(mdoc);
        return CLI_ERR;
    }
    yyjson_mut_doc_set_root(mdoc, root);

    /* Get or create "mcp" object */
    yyjson_mut_val *mcp = yyjson_mut_obj_get(root, "mcp");
    if (!mcp || !yyjson_mut_is_obj(mcp)) {
        mcp = yyjson_mut_obj(mdoc);
        yyjson_mut_obj_add_val(mdoc, root, "mcp", mcp);
    }

    yyjson_mut_obj_remove_key(mcp, "codebase-memory-mcp");

    yyjson_mut_val *entry = yyjson_mut_obj(mdoc);
    yyjson_mut_obj_add_bool(mdoc, entry, "enabled", true);
    yyjson_mut_obj_add_str(mdoc, entry, "type", "local");
    yyjson_mut_val *cmd_arr = yyjson_mut_arr(mdoc);
    yyjson_mut_arr_add_str(mdoc, cmd_arr, binary_path);
    yyjson_mut_obj_add_val(mdoc, entry, "command", cmd_arr);
    yyjson_mut_obj_add_val(mdoc, mcp, "codebase-memory-mcp", entry);

    int rc = write_json_file(config_path, mdoc);
    yyjson_mut_doc_free(mdoc);
    return rc;
}

int cbm_remove_opencode_mcp(const char *config_path) {
    if (!config_path) {
        return CLI_ERR;
    }

    yyjson_doc *doc = read_json_file(config_path);
    if (!doc) {
        return CLI_ERR;
    }

    yyjson_mut_doc *mdoc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_val_mut_copy(mdoc, yyjson_doc_get_root(doc));
    yyjson_doc_free(doc);
    if (!root) {
        yyjson_mut_doc_free(mdoc);
        return CLI_ERR;
    }
    yyjson_mut_doc_set_root(mdoc, root);

    yyjson_mut_val *mcp = yyjson_mut_obj_get(root, "mcp");
    if (!mcp || !yyjson_mut_is_obj(mcp)) {
        yyjson_mut_doc_free(mdoc);
        return 0;
    }

    yyjson_mut_obj_remove_key(mcp, "codebase-memory-mcp");

    int rc = write_json_file(config_path, mdoc);
    yyjson_mut_doc_free(mdoc);
    return rc;
}

/* ── Antigravity MCP config (JSON, same mcpServers format) ────── */

int cbm_upsert_antigravity_mcp(const char *binary_path, const char *config_path) {
    /* Antigravity uses same mcpServers format as Cursor/Gemini */
    return cbm_install_editor_mcp(binary_path, config_path);
}

int cbm_remove_antigravity_mcp(const char *config_path) {
    return cbm_remove_editor_mcp(config_path);
}

/* ── Claude Code pre-tool hooks ───────────────────────────────── */

#define CMM_HOOK_MATCHER "Grep|Glob|Read|Search"
#define CMM_HOOK_COMMAND "~/.claude/hooks/cbm-code-discovery-gate"

/* Old matcher values from previous versions — recognized during upgrade so
 * upsert_hooks_json can remove them before inserting the current matcher. */
static const char *cmm_old_matchers[] = {
    "Grep|Glob|Read",
    NULL,
};

/* Check if a PreToolUse array entry matches our hook (current or old matcher). */
static bool is_cmm_hook_entry(yyjson_mut_val *entry, const char *matcher_str) {
    yyjson_mut_val *matcher = yyjson_mut_obj_get(entry, "matcher");
    if (!matcher || !yyjson_mut_is_str(matcher)) {
        return false;
    }
    const char *val = yyjson_mut_get_str(matcher);
    if (!val) {
        return false;
    }
    if (strcmp(val, matcher_str) == 0) {
        return true;
    }
    /* Also match old versions for backwards-compatible upgrade */
    for (int i = 0; cmm_old_matchers[i]; i++) {
        if (strcmp(val, cmm_old_matchers[i]) == 0) {
            return true;
        }
    }
    return false;
}

/* Generic hook upsert for both Claude Code and Gemini CLI */

typedef struct {
    const char *settings_path;
    const char *hook_event;
    const char *matcher_str;
    const char *command_str;
} hooks_upsert_args_t;
static int upsert_hooks_json(hooks_upsert_args_t args) {
    const char *settings_path = args.settings_path;
    const char *hook_event = args.hook_event;
    const char *matcher_str = args.matcher_str;
    const char *command_str = args.command_str;
    if (!settings_path) {
        return CLI_ERR;
    }

    yyjson_mut_doc *mdoc = yyjson_mut_doc_new(NULL);
    if (!mdoc) {
        return CLI_ERR;
    }

    yyjson_doc *doc = read_json_file(settings_path);
    yyjson_mut_val *root;
    if (doc) {
        root = yyjson_val_mut_copy(mdoc, yyjson_doc_get_root(doc));
        yyjson_doc_free(doc);
    } else {
        root = yyjson_mut_obj(mdoc);
    }
    if (!root) {
        yyjson_mut_doc_free(mdoc);
        return CLI_ERR;
    }
    yyjson_mut_doc_set_root(mdoc, root);

    /* Get or create hooks object */
    yyjson_mut_val *hooks = yyjson_mut_obj_get(root, "hooks");
    if (!hooks || !yyjson_mut_is_obj(hooks)) {
        hooks = yyjson_mut_obj(mdoc);
        yyjson_mut_obj_add_val(mdoc, root, "hooks", hooks);
    }

    /* Get or create the hook event array (e.g. PreToolUse / BeforeTool) */
    yyjson_mut_val *event_arr = yyjson_mut_obj_get(hooks, hook_event);
    if (!event_arr || !yyjson_mut_is_arr(event_arr)) {
        event_arr = yyjson_mut_arr(mdoc);
        yyjson_mut_obj_add_val(mdoc, hooks, hook_event, event_arr);
    }

    /* Remove existing CMM entry if present */
    size_t idx;
    size_t max;
    yyjson_mut_val *item;
    yyjson_mut_arr_foreach(event_arr, idx, max, item) {
        if (is_cmm_hook_entry(item, matcher_str)) {
            yyjson_mut_arr_remove(event_arr, idx);
            break;
        }
    }

    /* Build our hook entry */
    yyjson_mut_val *entry = yyjson_mut_obj(mdoc);
    yyjson_mut_obj_add_str(mdoc, entry, "matcher", matcher_str);

    yyjson_mut_val *hooks_arr = yyjson_mut_arr(mdoc);
    yyjson_mut_val *hook_obj = yyjson_mut_obj(mdoc);
    yyjson_mut_obj_add_str(mdoc, hook_obj, "type", "command");
    yyjson_mut_obj_add_str(mdoc, hook_obj, "command", command_str);
    yyjson_mut_arr_append(hooks_arr, hook_obj);
    yyjson_mut_obj_add_val(mdoc, entry, "hooks", hooks_arr);

    yyjson_mut_arr_append(event_arr, entry);

    int rc = write_json_file(settings_path, mdoc);
    yyjson_mut_doc_free(mdoc);
    return rc;
}

/* Generic hook remove for both Claude Code and Gemini CLI */

typedef struct {
    const char *settings_path;
    const char *hook_event;
    const char *matcher_str;
} hooks_remove_args_t;
static int remove_hooks_json(hooks_remove_args_t args) {
    const char *settings_path = args.settings_path;
    const char *hook_event = args.hook_event;
    const char *matcher_str = args.matcher_str;
    if (!settings_path) {
        return CLI_ERR;
    }

    yyjson_doc *doc = read_json_file(settings_path);
    if (!doc) {
        return CLI_ERR;
    }

    yyjson_mut_doc *mdoc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_val_mut_copy(mdoc, yyjson_doc_get_root(doc));
    yyjson_doc_free(doc);
    if (!root) {
        yyjson_mut_doc_free(mdoc);
        return CLI_ERR;
    }
    yyjson_mut_doc_set_root(mdoc, root);

    yyjson_mut_val *hooks = yyjson_mut_obj_get(root, "hooks");
    if (!hooks) {
        yyjson_mut_doc_free(mdoc);
        return 0;
    }

    yyjson_mut_val *event_arr = yyjson_mut_obj_get(hooks, hook_event);
    if (!event_arr || !yyjson_mut_is_arr(event_arr)) {
        yyjson_mut_doc_free(mdoc);
        return 0;
    }

    size_t idx;
    size_t max;
    yyjson_mut_val *item;
    yyjson_mut_arr_foreach(event_arr, idx, max, item) {
        if (is_cmm_hook_entry(item, matcher_str)) {
            yyjson_mut_arr_remove(event_arr, idx);
            break;
        }
    }

    int rc = write_json_file(settings_path, mdoc);
    yyjson_mut_doc_free(mdoc);
    return rc;
}

int cbm_upsert_claude_hooks(const char *settings_path) {
    return upsert_hooks_json(
        (hooks_upsert_args_t){settings_path, "PreToolUse", CMM_HOOK_MATCHER, CMM_HOOK_COMMAND});
}

int cbm_remove_claude_hooks(const char *settings_path) {
    return remove_hooks_json((hooks_remove_args_t){settings_path, "PreToolUse", CMM_HOOK_MATCHER});
}

/* Install the code discovery gate script to ~/.claude/hooks/.
 * Blocks the first Grep/Glob/Read/Search call per session (exit 2 + stderr),
 * nudging Claude toward codebase-memory-mcp. All subsequent calls in the same
 * session pass through (gate file keyed on PPID). */
static void cbm_install_hook_gate_script(const char *home) {
    if (!home) {
        return;
    }
    char hooks_dir[CLI_BUF_1K];
    snprintf(hooks_dir, sizeof(hooks_dir), "%s/.claude/hooks", home);
    cbm_mkdir_p(hooks_dir, CLI_OCTAL_PERM);

    char script_path[CLI_BUF_1K];
    snprintf(script_path, sizeof(script_path), "%s/cbm-code-discovery-gate", hooks_dir);

    FILE *f = fopen(script_path, "w");
    if (!f) {
        return;
    }
    (void)fprintf(f, "#!/bin/bash\n"
                     "# Gate hook: nudges Claude toward codebase-memory-mcp for code discovery.\n"
                     "# First Grep/Glob/Read/Search per session -> block. Subsequent -> allow.\n"
                     "# PPID = Claude Code process PID, unique per session.\n"
                     "GATE=/tmp/cbm-code-discovery-gate-$PPID\n"
                     "find /tmp -name 'cbm-code-discovery-gate-*' -mtime +1 -delete 2>/dev/null\n"
                     "if [ -f \"$GATE\" ]; then\n"
                     "    exit 0\n"
                     "fi\n"
                     "touch \"$GATE\"\n"
                     "echo 'BLOCKED: For code discovery, use codebase-memory-mcp tools first: "
                     "search_graph(name_pattern) to find functions/classes, trace_path() for "
                     "call chains, get_code_snippet(qualified_name) to read source. If the graph "
                     "is not indexed yet, call index_repository first. Fall back to Grep/Glob/Read "
                     "only for text content search. If you need Grep, retry.' >&2\n"
                     "exit 2\n");
    /* fchmod before close to avoid TOCTOU race (CodeQL cpp/toctou-race-condition) */
#ifndef _WIN32
    fchmod(fileno(f), CLI_OCTAL_PERM);
#endif
    (void)fclose(f);
#ifdef _WIN32
    chmod(script_path, CLI_OCTAL_PERM);
#endif
}

/* SessionStart hook: remind agent to use MCP tools on every context reset. */
#define CMM_SESSION_COMMAND "~/.claude/hooks/cbm-session-reminder"

static void cbm_install_session_reminder_script(const char *home) {
    if (!home) {
        return;
    }
    char hooks_dir[CLI_BUF_1K];
    snprintf(hooks_dir, sizeof(hooks_dir), "%s/.claude/hooks", home);
    cbm_mkdir_p(hooks_dir, CLI_OCTAL_PERM);

    char script_path[CLI_BUF_1K];
    snprintf(script_path, sizeof(script_path), "%s/cbm-session-reminder", hooks_dir);

    FILE *f = fopen(script_path, "w");
    if (!f) {
        return;
    }
    (void)fprintf(
        f, "#!/bin/bash\n"
           "# SessionStart hook: remind agent to use codebase-memory-mcp tools.\n"
           "# Installed by codebase-memory-mcp. Fires on startup/resume/clear/compact.\n"
           "cat << 'REMINDER'\n"
           "CRITICAL - Code Discovery Protocol:\n"
           "1. ALWAYS use codebase-memory-mcp tools FIRST for ANY code exploration:\n"
           "   - search_graph(name_pattern/label/qn_pattern) to find functions/classes/routes\n"
           "   - trace_path(function_name, mode=calls|data_flow|cross_service) for call chains\n"
           "   - get_code_snippet(qualified_name) to read source (NOT Read/cat)\n"
           "   - query_graph(query) for complex Cypher patterns\n"
           "   - get_architecture(aspects) for project structure\n"
           "   - search_code(pattern) for text search (graph-augmented grep)\n"
           "2. Fall back to Grep/Glob/Read ONLY for text content, config values, non-code files.\n"
           "3. If a project is not indexed yet, run index_repository FIRST.\n"
           "REMINDER\n");
#ifndef _WIN32
    fchmod(fileno(f), CLI_OCTAL_PERM);
#endif
    (void)fclose(f);
#ifdef _WIN32
    chmod(script_path, CLI_OCTAL_PERM);
#endif
}

static int cbm_upsert_session_hooks(const char *settings_path) {
    static const char *matchers[] = {"startup", "resume", "clear", "compact"};
    int rc = 0;
    for (int i = 0; i < NUM_DIRS; i++) {
        if (upsert_hooks_json((hooks_upsert_args_t){settings_path, "SessionStart", matchers[i],
                                                    CMM_SESSION_COMMAND}) != 0) {
            rc = CLI_ERR;
        }
    }
    return rc;
}

static int cbm_remove_session_hooks(const char *settings_path) {
    static const char *matchers[] = {"startup", "resume", "clear", "compact"};
    int rc = 0;
    for (int i = 0; i < NUM_DIRS; i++) {
        if (remove_hooks_json((hooks_remove_args_t){settings_path, "SessionStart", matchers[i]}) !=
            0) {
            rc = CLI_ERR;
        }
    }
    return rc;
}

#define GEMINI_HOOK_MATCHER "google_search|read_file|grep_search"
#define GEMINI_HOOK_COMMAND                                               \
    "echo 'Reminder: prefer codebase-memory-mcp search_graph/trace_path/" \
    "get_code_snippet over grep/file search for code discovery.' >&2"

int cbm_upsert_gemini_hooks(const char *settings_path) {
    return upsert_hooks_json((hooks_upsert_args_t){settings_path, "BeforeTool", GEMINI_HOOK_MATCHER,
                                                   GEMINI_HOOK_COMMAND});
}

int cbm_remove_gemini_hooks(const char *settings_path) {
    return remove_hooks_json(
        (hooks_remove_args_t){settings_path, "BeforeTool", GEMINI_HOOK_MATCHER});
}

/* ── PATH management ──────────────────────────────────────────── */

int cbm_ensure_path(const char *bin_dir, const char *rc_file, bool dry_run) {
    if (!bin_dir || !rc_file) {
        return CLI_ERR;
    }

    char line[CLI_BUF_1K];
    snprintf(line, sizeof(line), "export PATH=\"%s:$PATH\"", bin_dir);

    /* Check if already present in rc file */
    FILE *f = fopen(rc_file, "r");
    if (f) {
        char buf[CLI_BUF_2K];
        while (fgets(buf, sizeof(buf), f)) {
            if (strstr(buf, line)) {
                (void)fclose(f);
                return CLI_TRUE; /* already present */
            }
        }
        (void)fclose(f);
    }

    if (dry_run) {
        return 0;
    }

    f = fopen(rc_file, "a");
    if (!f) {
        return CLI_ERR;
    }

    (void)fprintf(f, "\n# Added by codebase-memory-mcp install\n%s\n", line);
    (void)fclose(f);
    return 0;
}

/* ── Tar.gz extraction ────────────────────────────────────────── */

/* Decompress gzip data into a malloc'd buffer. Returns NULL on failure.
 * *out_total receives the decompressed size. Caller must free the result. */
static unsigned char *gzip_decompress(const unsigned char *data, int data_len, size_t *out_total) {
    z_stream strm = {0};
    unsigned char *mutable_data;
    memcpy(&mutable_data, &data, sizeof(data));
    strm.next_in = mutable_data;
    strm.avail_in = (unsigned int)data_len;

    if (inflateInit2(&strm, 16 + MAX_WBITS) != Z_OK) {
        return NULL;
    }

    size_t buf_cap = (size_t)data_len * DECOMP_FACTOR;
    if (buf_cap < CLI_BUF_4K) {
        buf_cap = CLI_BUF_4K;
    }
    if (buf_cap > DECOMPRESS_MAX_BYTES) {
        buf_cap = DECOMPRESS_MAX_BYTES;
    }
    unsigned char *decompressed = malloc(buf_cap);
    if (!decompressed) {
        inflateEnd(&strm);
        return NULL;
    }

    size_t total = 0;
    int ret;
    do {
        if (total >= buf_cap) {
            size_t new_cap = buf_cap * GROWTH_FACTOR;
            if (new_cap > DECOMPRESS_MAX_BYTES) {
                free(decompressed);
                inflateEnd(&strm);
                return NULL;
            }
            unsigned char *nb = realloc(decompressed, new_cap);
            if (!nb) {
                free(decompressed);
                inflateEnd(&strm);
                return NULL;
            }
            decompressed = nb;
            buf_cap = new_cap;
        }
        strm.next_out = decompressed + total;
        strm.avail_out = (unsigned int)(buf_cap - total);
        ret = inflate(&strm, Z_NO_FLUSH);
        total = buf_cap - strm.avail_out;
    } while (ret == Z_OK);

    inflateEnd(&strm);

    if (ret != Z_STREAM_END) {
        free(decompressed);
        return NULL;
    }
    *out_total = total;
    return decompressed;
}

/* Check if a tar block is all zeros (end of archive). */
static bool is_tar_end_of_archive(const unsigned char *hdr) {
    for (int i = 0; i < TAR_BLOCK_SIZE; i++) {
        if (hdr[i] != 0) {
            return false;
        }
    }
    return true;
}

/* Try to extract the target binary from a tar entry. Returns malloc'd data or NULL. */
static unsigned char *tar_try_extract_binary(const unsigned char *hdr, char typeflag,
                                             const char *name, const unsigned char *archive,
                                             size_t data_pos, long file_size, size_t total,
                                             int *out_len) {
    (void)hdr;
    if (typeflag != '0' && typeflag != '\0') {
        return NULL;
    }
    const char *basename = strrchr(name, '/');
    basename = basename ? basename + CLI_SKIP_ONE : name;
    if (strncmp(basename, TAR_BINARY_NAME, TAR_BINARY_NAME_LEN) != 0) {
        return NULL;
    }
    if (data_pos + (size_t)file_size > total) {
        return NULL;
    }
    unsigned char *result = malloc((size_t)file_size);
    if (!result) {
        return NULL;
    }
    memcpy(result, archive + data_pos, (size_t)file_size);
    *out_len = (int)file_size;
    return result;
}

unsigned char *cbm_extract_binary_from_targz(const unsigned char *data, int data_len,
                                             int *out_len) {
    if (!data || data_len <= 0 || !out_len) {
        return NULL;
    }

    size_t total = 0;
    unsigned char *decompressed = gzip_decompress(data, data_len, &total);
    if (!decompressed) {
        return NULL;
    }

    /* Parse tar: find entry starting with "codebase-memory-mcp" */
    size_t pos = 0;
    while (pos + TAR_BLOCK_SIZE <= total) {
        const unsigned char *hdr = decompressed + pos;

        if (is_tar_end_of_archive(hdr)) {
            break;
        }

        char name[TAR_NAME_LEN] = {0};
        memcpy(name, hdr, TAR_NAME_LEN - SKIP_ONE);
        char size_str[TAR_SIZE_LEN] = {0};
        memcpy(size_str, hdr + TAR_SIZE_OFFSET, TAR_SIZE_LEN - SKIP_ONE);
        long file_size = strtol(size_str, NULL, OCTAL_BASE);
        char typeflag = (char)hdr[TAR_TYPE_OFFSET];
        pos += TAR_BLOCK_SIZE;

        unsigned char *found = tar_try_extract_binary(hdr, typeflag, name, decompressed, pos,
                                                      file_size, total, out_len);
        if (found) {
            free(decompressed);
            return found;
        }

        size_t blocks = ((size_t)file_size + TAR_BLOCK_MASK) / TAR_BLOCK_SIZE;
        pos += blocks * TAR_BLOCK_SIZE;
    }

    free(decompressed);
    return NULL; /* binary not found */
}

/* ── Zip extraction (in-memory, replaces external unzip) ──────── */

/* Zip local file header constants */
enum {
    ZIP_SIG_0 = 0x50,
    ZIP_SIG_1 = 0x4B,
    ZIP_SIG_2 = 0x03,
    ZIP_SIG_3 = 0x04,
    ZIP_HDR_SZ = 30,
    ZIP_OFF_METHOD = 8,
    ZIP_OFF_COMP = 18,
    ZIP_OFF_UNCOMP = 22,
    ZIP_OFF_NAMELEN = 26,
    ZIP_OFF_EXTRALEN = 28,
    ZIP_STORED = 0,
    ZIP_DEFLATE = 8
};
static const uint32_t ZIP_MAX_UNCOMP = 500U * 1024U * 1024U;

/* Decompress a single zip entry (stored or deflated). Returns malloc'd buffer
 * or NULL on failure. *out_len receives the decompressed size. */
static unsigned char *zip_extract_entry(const unsigned char *file_data, uint16_t method,
                                        uint32_t comp_size, uint32_t uncomp_size, int *out_len) {
    if (method == ZIP_STORED) {
        if (comp_size > ZIP_MAX_UNCOMP) {
            return NULL;
        }
        unsigned char *out = malloc(comp_size);
        if (!out) {
            return NULL;
        }
        memcpy(out, file_data, comp_size);
        *out_len = (int)comp_size;
        return out;
    }
    if (method == ZIP_DEFLATE) {
        if (uncomp_size > ZIP_MAX_UNCOMP) {
            return NULL;
        }
        unsigned char *out = malloc(uncomp_size);
        if (!out) {
            return NULL;
        }
        z_stream strm = {0};
        strm.next_in = (unsigned char *)file_data;
        strm.avail_in = comp_size;
        strm.next_out = out;
        strm.avail_out = uncomp_size;
        if (inflateInit2(&strm, -MAX_WBITS) != Z_OK) {
            free(out);
            return NULL;
        }
        int ret = inflate(&strm, Z_FINISH);
        inflateEnd(&strm);
        if (ret != Z_STREAM_END) {
            free(out);
            return NULL;
        }
        *out_len = (int)strm.total_out;
        return out;
    }
    return NULL; /* unknown method */
}

unsigned char *cbm_extract_binary_from_zip(const unsigned char *data, int data_len, int *out_len) {
    if (!data || data_len <= 0 || !out_len) {
        return NULL;
    }
    *out_len = 0;

    int pos = 0;
    while (pos + ZIP_HDR_SZ <= data_len) {
        if (data[pos] != ZIP_SIG_0 || data[pos + CLI_SKIP_ONE] != ZIP_SIG_1 ||
            data[pos + CLI_PAIR_LEN] != ZIP_SIG_2 || data[pos + CLI_JSON_INDENT] != ZIP_SIG_3) {
            break;
        }

        uint16_t method = (uint16_t)(data[pos + ZIP_OFF_METHOD] |
                                     (data[pos + ZIP_OFF_METHOD + CLI_SKIP_ONE] << BYTE_SHIFT));
        uint32_t comp_size =
            (uint32_t)(data[pos + ZIP_OFF_COMP] |
                       (data[pos + ZIP_OFF_COMP + CLI_SKIP_ONE] << BYTE_SHIFT) |
                       (data[pos + ZIP_OFF_COMP + CLI_PAIR_LEN] << (BYTE_SHIFT * CLI_PAIR_LEN)) |
                       (data[pos + ZIP_OFF_COMP + CLI_JSON_INDENT]
                        << (BYTE_SHIFT * CLI_JSON_INDENT)));
        uint32_t uncomp_size =
            (uint32_t)(data[pos + ZIP_OFF_UNCOMP] |
                       (data[pos + ZIP_OFF_UNCOMP + CLI_SKIP_ONE] << BYTE_SHIFT) |
                       (data[pos + ZIP_OFF_UNCOMP + CLI_PAIR_LEN] << (BYTE_SHIFT * CLI_PAIR_LEN)) |
                       (data[pos + ZIP_OFF_UNCOMP + CLI_JSON_INDENT]
                        << (BYTE_SHIFT * CLI_JSON_INDENT)));
        uint16_t name_len = (uint16_t)(data[pos + ZIP_OFF_NAMELEN] |
                                       (data[pos + ZIP_OFF_NAMELEN + CLI_SKIP_ONE] << BYTE_SHIFT));
        uint16_t extra_len =
            (uint16_t)(data[pos + ZIP_OFF_EXTRALEN] |
                       (data[pos + ZIP_OFF_EXTRALEN + CLI_SKIP_ONE] << BYTE_SHIFT));

        int header_end = pos + ZIP_HDR_SZ + name_len + extra_len;
        if (header_end + (int)comp_size > data_len) {
            break;
        }

        char fname[CLI_BUF_512] = {0};
        int fn_copy = name_len < (int)sizeof(fname) - CLI_SKIP_ONE
                          ? name_len
                          : (int)sizeof(fname) - CLI_SKIP_ONE;
        memcpy(fname, data + pos + 30, (size_t)fn_copy);
        fname[fn_copy] = '\0';

        if (strstr(fname, "..")) {
            pos = header_end + (int)comp_size;
            continue;
        }

        const char *basename = strrchr(fname, '/');
        basename = basename ? basename + CLI_SKIP_ONE : fname;

        if (strcmp(basename, "codebase-memory-mcp") == 0 ||
            strcmp(basename, "codebase-memory-mcp.exe") == 0) {
            return zip_extract_entry(data + header_end, method, comp_size, uncomp_size, out_len);
        }

        pos = header_end + (int)comp_size;
    }

    return NULL;
}

/* ── Index management ─────────────────────────────────────────── */

static const char *get_cache_dir(const char *home_dir) {
    static char buf[CLI_BUF_1K];
    if (!home_dir) {
        home_dir = cbm_get_home_dir();
    }
    if (!home_dir) {
        return NULL;
    }
    snprintf(buf, sizeof(buf), "%s", cbm_resolve_cache_dir());
    return buf;
}

int cbm_list_indexes(const char *home_dir) {
    const char *cache_dir = get_cache_dir(home_dir);
    if (!cache_dir) {
        return 0;
    }

    cbm_dir_t *d = cbm_opendir(cache_dir);
    if (!d) {
        return 0;
    }

    int count = 0;
    cbm_dirent_t *ent;
    while ((ent = cbm_readdir(d)) != NULL) {
        size_t len = strlen(ent->name);
        if (len > DB_EXT_LEN && strcmp(ent->name + len - DB_EXT_LEN, ".db") == 0) {
            printf("  %s/%s\n", cache_dir, ent->name);
            count++;
        }
    }
    cbm_closedir(d);
    return count;
}

int cbm_remove_indexes(const char *home_dir) {
    const char *cache_dir = get_cache_dir(home_dir);
    if (!cache_dir) {
        return 0;
    }

    cbm_dir_t *d = cbm_opendir(cache_dir);
    if (!d) {
        return 0;
    }

    int count = 0;
    cbm_dirent_t *ent;
    while ((ent = cbm_readdir(d)) != NULL) {
        size_t len = strlen(ent->name);
        if (len > DB_EXT_LEN && strcmp(ent->name + len - DB_EXT_LEN, ".db") == 0) {
            char path[CLI_BUF_1K];
            snprintf(path, sizeof(path), "%s/%s", cache_dir, ent->name);
            /* Also remove .db.tmp if present */
            char tmp_path[CLI_FIELD_1040];
            snprintf(tmp_path, sizeof(tmp_path), "%s.tmp", path);
            cbm_unlink(tmp_path);
            if (cbm_unlink(path) == 0) {
                count++;
            }
        }
    }
    cbm_closedir(d);
    return count;
}

/* ── Config store (persistent key-value in _config.db) ─────────── */

#include <sqlite3.h>

struct cbm_config {
    sqlite3 *db;
    char get_buf[CLI_BUF_4K]; /* static buffer for cbm_config_get return values */
};

cbm_config_t *cbm_config_open(const char *cache_dir) {
    if (!cache_dir) {
        return NULL;
    }

    char dbpath[CLI_BUF_1K];
    snprintf(dbpath, sizeof(dbpath), "%s/_config.db", cache_dir);

    /* Ensure directory exists */
    mkdirp(cache_dir, DIR_PERMS);

    sqlite3 *db = NULL;
    if (sqlite3_open(dbpath, &db) != SQLITE_OK) {
        if (db) {
            sqlite3_close(db);
        }
        return NULL;
    }

    /* Create table if not exists */
    const char *sql = "CREATE TABLE IF NOT EXISTS config (key TEXT PRIMARY KEY, value TEXT)";
    char *err_msg = NULL;
    if (sqlite3_exec(db, sql, NULL, NULL, &err_msg) != SQLITE_OK) {
        sqlite3_free(err_msg);
        sqlite3_close(db);
        return NULL;
    }

    cbm_config_t *cfg = calloc(CBM_ALLOC_ONE, sizeof(*cfg));
    if (!cfg) {
        sqlite3_close(db);
        return NULL;
    }
    cfg->db = db;
    return cfg;
}

void cbm_config_close(cbm_config_t *cfg) {
    if (!cfg) {
        return;
    }
    if (cfg->db) {
        sqlite3_close(cfg->db);
    }
    free(cfg);
}

const char *cbm_config_get(cbm_config_t *cfg, const char *key, const char *default_val) {
    if (!cfg || !key) {
        return default_val;
    }

    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(cfg->db, "SELECT value FROM config WHERE key = ?", SQL_NUL_TERM, &stmt,
                           NULL) != SQLITE_OK) {
        return default_val;
    }
    sqlite3_bind_text(stmt, SQL_PARAM_1, key, SQL_NUL_TERM, cbm_sqlite_transient);

    const char *result = default_val;
    if (sqlite3_step(stmt) == SQLITE_ROW) {
        const char *val = (const char *)sqlite3_column_text(stmt, 0);
        if (val) {
            snprintf(cfg->get_buf, sizeof(cfg->get_buf), "%s", val);
            result = cfg->get_buf;
        }
    }
    sqlite3_finalize(stmt);
    return result;
}

bool cbm_config_get_bool(cbm_config_t *cfg, const char *key, bool default_val) {
    const char *val = cbm_config_get(cfg, key, NULL);
    if (!val) {
        return default_val;
    }
    if (strcmp(val, "true") == 0 || strcmp(val, "1") == 0 || strcmp(val, "on") == 0) {
        return true;
    }
    if (strcmp(val, "false") == 0 || strcmp(val, "0") == 0 || strcmp(val, "off") == 0) {
        return false;
    }
    return default_val;
}

int cbm_config_get_int(cbm_config_t *cfg, const char *key, int default_val) {
    const char *val = cbm_config_get(cfg, key, NULL);
    if (!val) {
        return default_val;
    }
    char *endptr;
    long v = strtol(val, &endptr, CLI_STRTOL_BASE);
    if (endptr == val || *endptr != '\0') {
        return default_val;
    }
    return (int)v;
}

int cbm_config_set(cbm_config_t *cfg, const char *key, const char *value) {
    if (!cfg || !key || !value) {
        return CLI_ERR;
    }

    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(cfg->db, "INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)",
                           SQL_NUL_TERM, &stmt, NULL) != SQLITE_OK) {
        return CLI_ERR;
    }
    sqlite3_bind_text(stmt, SQL_PARAM_1, key, SQL_NUL_TERM, cbm_sqlite_transient);
    sqlite3_bind_text(stmt, SQL_PARAM_2, value, SQL_NUL_TERM, cbm_sqlite_transient);

    int rc = sqlite3_step(stmt) == SQLITE_DONE ? 0 : CLI_ERR;
    sqlite3_finalize(stmt);
    return rc;
}

int cbm_config_delete(cbm_config_t *cfg, const char *key) {
    if (!cfg || !key) {
        return CLI_ERR;
    }

    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(cfg->db, "DELETE FROM config WHERE key = ?", SQL_NUL_TERM, &stmt,
                           NULL) != SQLITE_OK) {
        return CLI_ERR;
    }
    sqlite3_bind_text(stmt, SQL_PARAM_1, key, SQL_NUL_TERM, cbm_sqlite_transient);

    int rc = sqlite3_step(stmt) == SQLITE_DONE ? 0 : CLI_ERR;
    sqlite3_finalize(stmt);
    return rc;
}

/* ── Config CLI subcommand ────────────────────────────────────── */

int cbm_cmd_config(int argc, char **argv) {
    if (argc == 0) {
        printf("Usage: codebase-memory-mcp config <command> [args]\n\n");
        printf("Commands:\n");
        printf("  list             Show all config values\n");
        printf("  get <key>        Get a config value\n");
        printf("  set <key> <val>  Set a config value\n");
        printf("  reset <key>      Reset a key to default\n\n");
        printf("Config keys:\n");
        printf("  %-25s  default=%-10s  %s\n", CBM_CONFIG_AUTO_INDEX, "false",
               "Enable auto-indexing on MCP session start");
        printf("  %-25s  default=%-10s  %s\n", CBM_CONFIG_AUTO_INDEX_LIMIT, "50000",
               "Max files for auto-indexing new projects");
        return 0;
    }

    const char *home = cbm_get_home_dir();
    if (!home) {
        (void)fprintf(stderr, "error: HOME not set (use USERPROFILE on Windows)\n");
        return CLI_TRUE;
    }

    char cache_dir[CLI_BUF_1K];
    snprintf(cache_dir, sizeof(cache_dir), "%s", cbm_resolve_cache_dir());

    cbm_config_t *cfg = cbm_config_open(cache_dir);
    if (!cfg) {
        (void)fprintf(stderr, "error: cannot open config database\n");
        return CLI_TRUE;
    }

    int rc = 0;
    if (strcmp(argv[0], "list") == 0 || strcmp(argv[0], "ls") == 0) {
        printf("Configuration:\n");
        printf("  %-25s = %-10s\n", CBM_CONFIG_AUTO_INDEX,
               cbm_config_get(cfg, CBM_CONFIG_AUTO_INDEX, "false"));
        printf("  %-25s = %-10s\n", CBM_CONFIG_AUTO_INDEX_LIMIT,
               cbm_config_get(cfg, CBM_CONFIG_AUTO_INDEX_LIMIT, "50000"));
    } else if (strcmp(argv[0], "get") == 0) {
        if (argc < MIN_ARGC_GET) {
            (void)fprintf(stderr, "Usage: config get <key>\n");
            rc = CLI_TRUE;
        } else {
            printf("%s\n", cbm_config_get(cfg, argv[CLI_SKIP_ONE], ""));
        }
    } else if (strcmp(argv[0], "set") == 0) {
        if (argc < MIN_ARGC_CMD) {
            (void)fprintf(stderr, "Usage: config set <key> <value>\n");
            rc = CLI_TRUE;
        } else {
            if (cbm_config_set(cfg, argv[CLI_SKIP_ONE], argv[CLI_PAIR_LEN]) == 0) {
                printf("%s = %s\n", argv[CLI_SKIP_ONE], argv[CLI_PAIR_LEN]);
            } else {
                (void)fprintf(stderr, "error: failed to set %s\n", argv[CLI_SKIP_ONE]);
                rc = CLI_TRUE;
            }
        }
    } else if (strcmp(argv[0], "reset") == 0) {
        if (argc < MIN_ARGC_GET) {
            (void)fprintf(stderr, "Usage: config reset <key>\n");
            rc = CLI_TRUE;
        } else {
            cbm_config_delete(cfg, argv[CLI_SKIP_ONE]);
            printf("%s reset to default\n", argv[CLI_SKIP_ONE]);
        }
    } else {
        (void)fprintf(stderr, "Unknown config command: %s\n", argv[0]);
        rc = CLI_TRUE;
    }

    cbm_config_close(cfg);
    return rc;
}

/* ── Interactive prompt ───────────────────────────────────────── */

/* Global auto-answer mode: 0=interactive, 1=always yes, -1=always no */
static int g_auto_answer = 0;

static void parse_auto_answer(int argc, char **argv) {
    for (int i = 0; i < argc; i++) {
        if (strcmp(argv[i], "-y") == 0 || strcmp(argv[i], "--yes") == 0) {
            g_auto_answer = AUTO_YES;
        }
        if (strcmp(argv[i], "-n") == 0 || strcmp(argv[i], "--no") == 0) {
            g_auto_answer = AUTO_NO;
        }
    }
}

static bool prompt_yn(const char *question) {
    if (g_auto_answer == AUTO_YES) {
        printf("%s (y/n): y (auto)\n", question);
        return true;
    }
    if (g_auto_answer == AUTO_NO) {
        printf("%s (y/n): n (auto)\n", question);
        return false;
    }

    /* Non-interactive stdin: default to "no" to avoid hanging */
#ifndef _WIN32
    if (!isatty(fileno(stdin))) {
        (void)fprintf(stderr,
                      "error: interactive prompt requires a terminal. Use -y or -n flags.\n");
        return false;
    }
#endif

    printf("%s (y/n): ", question);
    (void)fflush(stdout);

    char buf[CLI_BUF_16];
    if (!fgets(buf, sizeof(buf), stdin)) {
        return false;
    }
    return (buf[0] == 'y' || buf[0] == 'Y') ? true : false;
}

/* ── SHA-CBM_SZ_256 checksum verification ─────────────────────────────── */

/* SHA-CBM_SZ_256 hex digest: CBM_SZ_64 hex chars + NUL */
#define SHA256_HEX_LEN CBM_SZ_64
#define SHA256_BUF_SIZE (SHA256_HEX_LEN + CLI_SKIP_ONE)
/* Minimum line length in checksums.txt: CBM_SZ_64 hex + 2 spaces + 1 char filename */
#define CHECKSUM_LINE_MIN (SHA256_HEX_LEN + 2)

/* Compute SHA-CBM_SZ_256 of a file using platform tools (sha256sum/shasum).
 * Writes CBM_SZ_64-char hex digest + NUL to out. Returns 0 on success. */
static int sha256_file(const char *path, char *out, size_t out_size) {
    if (out_size < SHA256_BUF_SIZE) {
        return CLI_ERR;
    }
    char cmd[CLI_BUF_1K];
#ifdef __APPLE__
    snprintf(cmd, sizeof(cmd), "shasum -a CBM_SZ_256 '%s' 2>/dev/null", path);
#else
    snprintf(cmd, sizeof(cmd), "sha256sum '%s' 2>/dev/null", path);
#endif
    FILE *fp = cbm_popen(cmd, "r");
    if (!fp) {
        return CLI_ERR;
    }
    char line[CLI_BUF_256];
    if (fgets(line, sizeof(line), fp)) {
        /* Output format: <CBM_SZ_64-char hash>  <filename> */
        char *space = strchr(line, ' ');
        if (space && space - line == SHA256_HEX_LEN) {
            memcpy(out, line, SHA256_HEX_LEN);
            out[SHA256_HEX_LEN] = '\0';
            cbm_pclose(fp);
            return 0;
        }
    }
    cbm_pclose(fp);
    return CLI_ERR;
}

/* ── Download helper (shell-free curl via exec) ───────────────── */

static int cbm_download_to_file(const char *url, const char *dest) {
    const char *argv[] = {"curl", "-fSL", "--progress-bar", "-o", dest, url, NULL};
    return cbm_exec_no_shell(argv);
}

static int cbm_download_to_file_quiet(const char *url, const char *dest) {
    const char *argv[] = {"curl", "-fsSL", "-o", dest, url, NULL};
    return cbm_exec_no_shell(argv);
}

/* ── macOS ad-hoc signing ─────────────────────────────────────── */

#ifdef __APPLE__
static int cbm_macos_adhoc_sign(const char *binary_path) {
    /* Remove quarantine xattr (best effort — may not exist) */
    const char *xattr_argv[] = {"xattr", "-d", "com.apple.quarantine", binary_path, NULL};
    (void)cbm_exec_no_shell(xattr_argv);

    /* Ad-hoc sign (required for arm64, harmless for x86_64) */
    const char *sign_argv[] = {"codesign", "--sign", "-", "--force", binary_path, NULL};
    return cbm_exec_no_shell(sign_argv);
}
#endif

/* ── Kill other MCP server instances ──────────────────────────── */

static int cbm_kill_other_instances(void) {
#ifdef _WIN32
    /* taskkill /IM kills ALL matching processes INCLUDING self.
     * Use /FI filter to exclude our own PID. */
    char pid_filter[CBM_SZ_64];
    snprintf(pid_filter, sizeof(pid_filter), "PID ne %lu", (unsigned long)GetCurrentProcessId());
    const char *argv[] = {"taskkill", "/F",       "/FI", "IMAGENAME eq codebase-memory-mcp.exe",
                          "/FI",      pid_filter, NULL};
    (void)cbm_exec_no_shell(argv);
    return 0;
#else
    int killed = 0;
    pid_t self = getpid();
    FILE *fp = cbm_popen("pgrep -x codebase-memory-mcp", "r");
    if (!fp) {
        return 0;
    }
    char line[CLI_BUF_32];
    while (fgets(line, sizeof(line), fp)) {
        pid_t pid = (pid_t)strtol(line, NULL, CLI_STRTOL_BASE);
        if (pid > 0 && pid != self) {
            if (kill(pid, SIGTERM) == 0) {
                killed++;
            }
        }
    }
    cbm_pclose(fp);
    return killed;
#endif
}

/* Download checksums.txt and verify the archive integrity.
 * Returns: 0 = verified OK, 1 = mismatch (FAIL), -1 = could not verify (warning). */
static int verify_download_checksum(const char *archive_path, const char *archive_name) {
    char checksum_file[CLI_BUF_256];
    snprintf(checksum_file, sizeof(checksum_file), "%s/cbm-checksums.txt", cbm_tmpdir());

    char dl_base_buf[CLI_BUF_512];
    const char *dl_base =
        cbm_safe_getenv("CBM_DOWNLOAD_URL", dl_base_buf, sizeof(dl_base_buf), NULL);
    char checksum_url[CLI_BUF_512];
    if (dl_base && dl_base[0]) {
        snprintf(checksum_url, sizeof(checksum_url), "%s/checksums.txt", dl_base);
    } else {
        snprintf(checksum_url, sizeof(checksum_url), "%s",
                 "https://github.com/DeusData/codebase-memory-mcp/releases/latest/download/"
                 "checksums.txt");
    }
    int rc = cbm_download_to_file_quiet(checksum_url, checksum_file);
    if (rc != 0) {
        (void)fprintf(stderr,
                      "warning: could not download checksums.txt — skipping verification\n");
        cbm_unlink(checksum_file);
        return CLI_ERR;
    }

    FILE *fp = fopen(checksum_file, "r");
    cbm_unlink(checksum_file);
    if (!fp) {
        return CLI_ERR;
    }

    char expected[SHA256_BUF_SIZE] = {0};
    char line[CLI_BUF_512];
    while (fgets(line, sizeof(line), fp)) {
        /* Format: <CBM_SZ_64-char sha256>  <filename>\n */
        if (strlen(line) > CHECKSUM_LINE_MIN && strstr(line, archive_name)) {
            memcpy(expected, line, SHA256_HEX_LEN);
            expected[SHA256_HEX_LEN] = '\0';
            break;
        }
    }
    (void)fclose(fp);

    if (expected[0] == '\0') {
        (void)fprintf(stderr, "warning: %s not found in checksums.txt\n", archive_name);
        return CLI_ERR;
    }

    char actual[SHA256_BUF_SIZE] = {0};
    if (sha256_file(archive_path, actual, sizeof(actual)) != 0) {
        (void)fprintf(stderr, "warning: sha256sum/shasum not available — skipping verification\n");
        return CLI_ERR;
    }

    if (strcmp(expected, actual) != 0) {
        (void)fprintf(stderr, "error: CHECKSUM MISMATCH — downloaded binary may be compromised!\n");
        (void)fprintf(stderr, "  expected: %s\n", expected);
        (void)fprintf(stderr, "  actual:   %s\n", actual);
        return CLI_TRUE;
    }

    printf("Checksum verified: %s\n", actual);
    return 0;
}

/* ── Detect OS/arch for download URL ──────────────────────────── */

static const char *detect_os(void) {
#ifdef _WIN32
    return "windows";
#elif defined(__APPLE__)
    return "darwin";
#else
    return "linux";
#endif
}

static const char *detect_arch(void) {
#if defined(__aarch64__) || defined(_M_ARM64)
    return "arm64";
#else
    return "amd64";
#endif
}

/* ── Agent config install/refresh (shared by install + update) ── */

/* Print detected agent names on a single line. */
static void print_detected_agents(const cbm_detected_agents_t *a) {
    struct {
        bool flag;
        const char *name;
    } agents[] = {
        {a->claude_code, "Claude-Code"},
        {a->codex, "Codex"},
        {a->gemini, "Gemini-CLI"},
        {a->zed, "Zed"},
        {a->opencode, "OpenCode"},
        {a->antigravity, "Antigravity"},
        {a->aider, "Aider"},
        {a->kilocode, "KiloCode"},
        {a->vscode, "VS-Code"},
        {a->openclaw, "OpenClaw"},
    };
    printf("Detected agents:");
    bool any = false;
    for (int i = 0; i < (int)(sizeof(agents) / sizeof(agents[0])); i++) {
        if (agents[i].flag) {
            printf(" %s", agents[i].name);
            any = true;
        }
    }
    if (!any) {
        printf(" (none)");
    }
    printf("\n\n");
}

/* Install Claude Code-specific configs (skills, MCP, hooks). */
static void install_claude_code_config(const char *home, const char *binary_path, bool force,
                                       bool dry_run) {
    char skills_dir[CLI_BUF_1K];
    snprintf(skills_dir, sizeof(skills_dir), "%s/.claude/skills", home);
    printf("Claude Code:\n");

    int skill_count = cbm_install_skills(skills_dir, force, dry_run);
    printf("  skills: %d installed\n", skill_count);

    if (cbm_remove_old_monolithic_skill(skills_dir, dry_run)) {
        printf("  removed old monolithic skill\n");
    }

    char mcp_path[CLI_BUF_1K];
    snprintf(mcp_path, sizeof(mcp_path), "%s/.claude/.mcp.json", home);
    if (!dry_run) {
        cbm_install_editor_mcp(binary_path, mcp_path);
    }
    printf("  mcp: %s\n", mcp_path);

    char mcp_path2[CLI_BUF_1K];
    snprintf(mcp_path2, sizeof(mcp_path2), "%s/.claude.json", home);
    if (!dry_run) {
        cbm_install_editor_mcp(binary_path, mcp_path2);
    }
    printf("  mcp: %s\n", mcp_path2);

    char settings_path[CLI_BUF_1K];
    snprintf(settings_path, sizeof(settings_path), "%s/.claude/settings.json", home);
    if (!dry_run) {
        cbm_upsert_claude_hooks(settings_path);
        cbm_install_hook_gate_script(home);
        cbm_install_session_reminder_script(home);
        cbm_upsert_session_hooks(settings_path);
    }
    printf("  hooks: PreToolUse (code discovery gate)\n");
    printf("  hooks: SessionStart (MCP usage reminder on startup/resume/clear/compact)\n");
}

/* Install MCP config + optional instructions for a generic agent. */
static void install_generic_agent_config(const char *label, const char *binary_path,
                                         const char *config_path, const char *instr_path,
                                         bool dry_run,
                                         int (*install_mcp)(const char *, const char *)) {
    printf("%s:\n", label);
    if (!dry_run) {
        install_mcp(binary_path, config_path);
    }
    printf("  mcp: %s\n", config_path);
    if (instr_path) {
        if (!dry_run) {
            cbm_upsert_instructions(instr_path, agent_instructions_content);
        }
        printf("  instructions: %s\n", instr_path);
    }
}

/* Install MCP configs for CLI-based agents (Codex, Gemini, OpenCode, Antigravity, Aider). */
/* Install Gemini CLI config with hooks. */
static void install_gemini_config(const char *home, const char *binary_path, bool dry_run) {
    char cp[CLI_BUF_1K];
    char ip[CLI_BUF_1K];
    snprintf(cp, sizeof(cp), "%s/.gemini/settings.json", home);
    snprintf(ip, sizeof(ip), "%s/.gemini/GEMINI.md", home);
    install_generic_agent_config("Gemini CLI", binary_path, cp, ip, dry_run,
                                 cbm_install_editor_mcp);
    if (!dry_run) {
        cbm_upsert_gemini_hooks(cp);
    }
    printf("  hooks: BeforeTool (grep/file search reminder)\n");
}

static void install_cli_agent_configs(const cbm_detected_agents_t *agents, const char *home,
                                      const char *binary_path, bool dry_run) {
    if (agents->codex) {
        char cp[CLI_BUF_1K];
        char ip[CLI_BUF_1K];
        snprintf(cp, sizeof(cp), "%s/.codex/config.toml", home);
        snprintf(ip, sizeof(ip), "%s/.codex/AGENTS.md", home);
        install_generic_agent_config("Codex CLI", binary_path, cp, ip, dry_run,
                                     cbm_upsert_codex_mcp);
    }
    if (agents->gemini) {
        install_gemini_config(home, binary_path, dry_run);
    }
    if (agents->opencode) {
        char cp[CLI_BUF_1K];
        char ip[CLI_BUF_1K];
        snprintf(cp, sizeof(cp), "%s/.config/opencode/opencode.json", home);
        snprintf(ip, sizeof(ip), "%s/.config/opencode/AGENTS.md", home);
        install_generic_agent_config("OpenCode", binary_path, cp, ip, dry_run,
                                     cbm_upsert_opencode_mcp);
    }
    if (agents->antigravity) {
        char cp[CLI_BUF_1K];
        char ip[CLI_BUF_1K];
        snprintf(cp, sizeof(cp), "%s/.gemini/antigravity/mcp_config.json", home);
        snprintf(ip, sizeof(ip), "%s/.gemini/antigravity/AGENTS.md", home);
        install_generic_agent_config("Antigravity", binary_path, cp, ip, dry_run,
                                     cbm_upsert_antigravity_mcp);
    }
    if (agents->aider) {
        char ip[CLI_BUF_1K];
        snprintf(ip, sizeof(ip), "%s/CONVENTIONS.md", home);
        printf("Aider:\n");
        if (!dry_run) {
            cbm_upsert_instructions(ip, agent_instructions_content);
        }
        printf("  instructions: %s\n", ip);
    }
}

/* Install MCP configs for editor-based agents (Zed, KiloCode, VS Code, OpenClaw). */
static void install_editor_agent_configs(const cbm_detected_agents_t *agents, const char *home,
                                         const char *binary_path, bool dry_run) {
    if (agents->zed) {
        char cp[CLI_BUF_1K];
#ifdef __APPLE__
        snprintf(cp, sizeof(cp), "%s/Library/Application Support/Zed/settings.json", home);
#elif defined(_WIN32)
        snprintf(cp, sizeof(cp), "%s/Zed/settings.json", cbm_app_local_dir());
#else
        snprintf(cp, sizeof(cp), "%s/zed/settings.json", cbm_app_config_dir());
#endif
        install_generic_agent_config("Zed", binary_path, cp, NULL, dry_run, cbm_install_zed_mcp);
    }
    if (agents->kilocode) {
        char cp[CLI_BUF_1K];
        char ip[CLI_BUF_1K];
#ifdef __APPLE__
        snprintf(cp, sizeof(cp),
                 "%s/Library/Application Support/Code/User/globalStorage/"
                 "kilocode.kilo-code/settings/mcp_settings.json",
                 home);
#else
        snprintf(cp, sizeof(cp),
                 "%s/Code/User/globalStorage/kilocode.kilo-code/settings/mcp_settings.json",
                 cbm_app_config_dir());
#endif
        snprintf(ip, sizeof(ip), "%s/.kilocode/rules/codebase-memory-mcp.md", home);
        install_generic_agent_config("KiloCode", binary_path, cp, ip, dry_run,
                                     cbm_install_editor_mcp);
    }
    if (agents->vscode) {
        char cp[CLI_BUF_1K];
#ifdef __APPLE__
        snprintf(cp, sizeof(cp), "%s/Library/Application Support/Code/User/mcp.json", home);
#else
        snprintf(cp, sizeof(cp), "%s/Code/User/mcp.json", cbm_app_config_dir());
#endif
        install_generic_agent_config("VS Code", binary_path, cp, NULL, dry_run,
                                     cbm_install_vscode_mcp);
    }
    if (agents->openclaw) {
        char cp[CLI_BUF_1K];
        snprintf(cp, sizeof(cp), "%s/.openclaw/openclaw.json", home);
        install_generic_agent_config("OpenClaw", binary_path, cp, NULL, dry_run,
                                     cbm_install_editor_mcp);
    }
}

static void cbm_install_agent_configs(const char *home, const char *binary_path, bool force,
                                      bool dry_run) {
    cbm_detected_agents_t agents = cbm_detect_agents(home);
    print_detected_agents(&agents);

    if (agents.claude_code) {
        install_claude_code_config(home, binary_path, force, dry_run);
    }
    install_cli_agent_configs(&agents, home, binary_path, dry_run);
    install_editor_agent_configs(&agents, home, binary_path, dry_run);
}

/* Count .db files in the cache directory. */
static int count_db_indexes(const char *home) {
    const char *cache_dir = get_cache_dir(home);
    if (!cache_dir) {
        return 0;
    }
    cbm_dir_t *d = cbm_opendir(cache_dir);
    if (!d) {
        return 0;
    }
    int count = 0;
    cbm_dirent_t *ent;
    while ((ent = cbm_readdir(d)) != NULL) {
        size_t len = strlen(ent->name);
        if (len > DB_EXT_LEN && strcmp(ent->name + len - DB_EXT_LEN, ".db") == 0) {
            count++;
        }
    }
    cbm_closedir(d);
    return count;
}

/* ── Subcommand: install ──────────────────────────────────────── */

/* Detect the running binary's path at runtime. Falls back to ~/.local/bin/. */
static void cbm_detect_self_path(char *buf, size_t buf_sz, const char *home) {
    buf[0] = '\0';
#ifdef _WIN32
    GetModuleFileNameA(NULL, buf, (DWORD)buf_sz);
    cbm_normalize_path_sep(buf);
#elif defined(__APPLE__)
    uint32_t sp_sz = (uint32_t)buf_sz;
    if (_NSGetExecutablePath(buf, &sp_sz) != 0) {
        buf[0] = '\0';
    }
#else
    ssize_t sp_len = readlink("/proc/self/exe", buf, buf_sz - SKIP_ONE);
    if (sp_len > 0) {
        buf[sp_len] = '\0';
    }
#endif
    if (!buf[0]) {
#ifdef _WIN32
        snprintf(buf, buf_sz, "%s/.local/bin/codebase-memory-mcp.exe", home);
#else
        snprintf(buf, buf_sz, "%s/.local/bin/codebase-memory-mcp", home);
#endif
    }
}

int cbm_cmd_install(int argc, char **argv) {
    parse_auto_answer(argc, argv);
    bool dry_run = false;
    bool force = false;
    for (int i = 0; i < argc; i++) {
        if (strcmp(argv[i], "--dry-run") == 0) {
            dry_run = true;
        }
        if (strcmp(argv[i], "--force") == 0) {
            force = true;
        }
    }

    const char *home = cbm_get_home_dir();
    if (!home) {
        (void)fprintf(stderr, "error: HOME not set (use USERPROFILE on Windows)\n");
        return CLI_TRUE;
    }

    printf("codebase-memory-mcp install %s\n\n", CBM_VERSION);

    int index_count = count_db_indexes(home);
    if (index_count > 0) {
        printf("Found %d existing index(es) that must be rebuilt:\n", index_count);
        cbm_list_indexes(home);
        printf("\n");
        if (!prompt_yn("Delete these indexes and continue with install?")) {
            printf("Install cancelled.\n");
            return CLI_TRUE;
        }
        if (!dry_run) {
            int removed = cbm_remove_indexes(home);
            printf("Removed %d index(es).\n\n", removed);
        }
    }

    /* Step 1b: Kill running MCP server instances so agents pick up new config */
    if (!dry_run) {
        int killed = cbm_kill_other_instances();
        if (killed > 0) {
            printf("Stopped %d running MCP server instance(s).\n\n", killed);
        }
    }

    /* Step 1c: macOS ad-hoc signing (in case binary was placed without signing) */
#ifdef __APPLE__
    {
        char sign_path[CLI_BUF_1K];
        snprintf(sign_path, sizeof(sign_path), "%s/.local/bin/codebase-memory-mcp", home);
        struct stat sign_st;
        if (stat(sign_path, &sign_st) == 0) {
            (void)cbm_macos_adhoc_sign(sign_path);
        }
    }
#endif

    /* Step 2: Binary path — detect actual location at runtime. */
    char self_path[CLI_BUF_1K] = {0};
    cbm_detect_self_path(self_path, sizeof(self_path), home);

    /* Step 3: Install/refresh all agent configs */
    cbm_install_agent_configs(home, self_path, force, dry_run);

    /* Step 4: Ensure PATH */
    char bin_dir[CLI_BUF_1K];
    snprintf(bin_dir, sizeof(bin_dir), "%s/.local/bin", home);
    const char *rc = cbm_detect_shell_rc(home);
    if (rc[0]) {
        int path_rc = cbm_ensure_path(bin_dir, rc, dry_run);
        if (path_rc == 0) {
            printf("\nAdded %s to PATH in %s\n", bin_dir, rc);
        } else if (path_rc == CLI_TRUE) {
            printf("\nPATH already includes %s\n", bin_dir);
        }
    }

    printf("\nInstall complete. Restart your shell or run:\n");
    printf("  source %s\n", rc);
    if (dry_run) {
        printf("\n(dry-run — no files were modified)\n");
    }
    return 0;
}

/* ── Subcommand: uninstall ────────────────────────────────────── */

/* Remove Claude Code agent configs. */
static void uninstall_claude_code(const char *home, bool dry_run) {
    char skills_dir[CLI_BUF_1K];
    snprintf(skills_dir, sizeof(skills_dir), "%s/.claude/skills", home);
    int removed = cbm_remove_skills(skills_dir, dry_run);
    printf("Claude Code: removed %d skill(s)\n", removed);

    char mcp_path[CLI_BUF_1K];
    snprintf(mcp_path, sizeof(mcp_path), "%s/.claude/.mcp.json", home);
    if (!dry_run) {
        cbm_remove_editor_mcp(mcp_path);
    }
    printf("  removed MCP config entry\n");

    char mcp_path2[CLI_BUF_1K];
    snprintf(mcp_path2, sizeof(mcp_path2), "%s/.claude.json", home);
    if (!dry_run) {
        cbm_remove_editor_mcp(mcp_path2);
    }

    char settings_path[CLI_BUF_1K];
    snprintf(settings_path, sizeof(settings_path), "%s/.claude/settings.json", home);
    if (!dry_run) {
        cbm_remove_claude_hooks(settings_path);
        cbm_remove_session_hooks(settings_path);
    }
    printf("  removed PreToolUse + SessionStart hooks\n");
}

/* Remove MCP + instructions for a generic agent. */

typedef struct {
    const char *name;
    const char *config_path;
    const char *instr_path;
} mcp_uninstall_args_t;
static void uninstall_agent_mcp_instr(mcp_uninstall_args_t paths, bool dry_run,
                                      int (*remove_fn)(const char *)) {
    const char *name = paths.name;
    const char *instr_path = paths.instr_path;
    if (!dry_run) {
        remove_fn(paths.config_path);
    }
    printf("%s: removed MCP config entry\n", name);
    if (instr_path) {
        if (!dry_run) {
            cbm_remove_instructions(instr_path);
        }
        printf("  removed instructions\n");
    }
}

/* Remove CLI agent configs (Codex, Gemini, OpenCode, Antigravity, Aider). */
/* Uninstall Gemini CLI config + hooks. */
static void uninstall_gemini_config(const char *home, bool dry_run) {
    char cp[CLI_BUF_1K];
    char ip[CLI_BUF_1K];
    snprintf(cp, sizeof(cp), "%s/.gemini/settings.json", home);
    snprintf(ip, sizeof(ip), "%s/.gemini/GEMINI.md", home);
    if (!dry_run) {
        cbm_remove_editor_mcp(cp);
        cbm_remove_gemini_hooks(cp);
        cbm_remove_instructions(ip);
    }
    printf("Gemini CLI: removed MCP config + hooks + instructions\n");
}

static void uninstall_cli_agents(const cbm_detected_agents_t *agents, const char *home,
                                 bool dry_run) {
    if (agents->codex) {
        char cp[CLI_BUF_1K];
        char ip[CLI_BUF_1K];
        snprintf(cp, sizeof(cp), "%s/.codex/config.toml", home);
        snprintf(ip, sizeof(ip), "%s/.codex/AGENTS.md", home);
        uninstall_agent_mcp_instr((mcp_uninstall_args_t){"Codex CLI", cp, ip}, dry_run,
                                  cbm_remove_codex_mcp);
    }
    if (agents->gemini) {
        uninstall_gemini_config(home, dry_run);
    }
    if (agents->opencode) {
        char cp[CLI_BUF_1K];
        char ip[CLI_BUF_1K];
        snprintf(cp, sizeof(cp), "%s/.config/opencode/opencode.json", home);
        snprintf(ip, sizeof(ip), "%s/.config/opencode/AGENTS.md", home);
        uninstall_agent_mcp_instr((mcp_uninstall_args_t){"OpenCode", cp, ip}, dry_run,
                                  cbm_remove_opencode_mcp);
    }
    if (agents->antigravity) {
        char cp[CLI_BUF_1K];
        char ip[CLI_BUF_1K];
        snprintf(cp, sizeof(cp), "%s/.gemini/antigravity/mcp_config.json", home);
        snprintf(ip, sizeof(ip), "%s/.gemini/antigravity/AGENTS.md", home);
        uninstall_agent_mcp_instr((mcp_uninstall_args_t){"Antigravity", cp, ip}, dry_run,
                                  cbm_remove_antigravity_mcp);
    }
    if (agents->aider) {
        char ip[CLI_BUF_1K];
        snprintf(ip, sizeof(ip), "%s/CONVENTIONS.md", home);
        if (!dry_run) {
            cbm_remove_instructions(ip);
        }
        printf("Aider: removed instructions\n");
    }
}

/* Remove editor agent configs (Zed, KiloCode, VS Code, OpenClaw). */
static void uninstall_editor_agents(const cbm_detected_agents_t *agents, const char *home,
                                    bool dry_run) {
    if (agents->zed) {
        char cp[CLI_BUF_1K];
#ifdef __APPLE__
        snprintf(cp, sizeof(cp), "%s/Library/Application Support/Zed/settings.json", home);
#elif defined(_WIN32)
        snprintf(cp, sizeof(cp), "%s/Zed/settings.json", cbm_app_local_dir());
#else
        snprintf(cp, sizeof(cp), "%s/zed/settings.json", cbm_app_config_dir());
#endif
        uninstall_agent_mcp_instr((mcp_uninstall_args_t){"Zed", cp, NULL}, dry_run,
                                  cbm_remove_zed_mcp);
    }
    if (agents->kilocode) {
        char cp[CLI_BUF_1K];
        char ip[CLI_BUF_1K];
#ifdef __APPLE__
        snprintf(cp, sizeof(cp),
                 "%s/Library/Application Support/Code/User/globalStorage/"
                 "kilocode.kilo-code/settings/mcp_settings.json",
                 home);
#else
        snprintf(cp, sizeof(cp),
                 "%s/Code/User/globalStorage/kilocode.kilo-code/settings/mcp_settings.json",
                 cbm_app_config_dir());
#endif
        snprintf(ip, sizeof(ip), "%s/.kilocode/rules/codebase-memory-mcp.md", home);
        uninstall_agent_mcp_instr((mcp_uninstall_args_t){"KiloCode", cp, ip}, dry_run,
                                  cbm_remove_editor_mcp);
    }
    if (agents->vscode) {
        char cp[CLI_BUF_1K];
#ifdef __APPLE__
        snprintf(cp, sizeof(cp), "%s/Library/Application Support/Code/User/mcp.json", home);
#else
        snprintf(cp, sizeof(cp), "%s/Code/User/mcp.json", cbm_app_config_dir());
#endif
        uninstall_agent_mcp_instr((mcp_uninstall_args_t){"VS Code", cp, NULL}, dry_run,
                                  cbm_remove_vscode_mcp);
    }
    if (agents->openclaw) {
        char cp[CLI_BUF_1K];
        snprintf(cp, sizeof(cp), "%s/.openclaw/openclaw.json", home);
        uninstall_agent_mcp_instr((mcp_uninstall_args_t){"OpenClaw", cp, NULL}, dry_run,
                                  cbm_remove_editor_mcp);
    }
}

int cbm_cmd_uninstall(int argc, char **argv) {
    parse_auto_answer(argc, argv);
    bool dry_run = false;
    for (int i = 0; i < argc; i++) {
        if (strcmp(argv[i], "--dry-run") == 0) {
            dry_run = true;
        }
    }

    const char *home = cbm_get_home_dir();
    if (!home) {
        (void)fprintf(stderr, "error: HOME not set (use USERPROFILE on Windows)\n");
        return CLI_TRUE;
    }

    printf("codebase-memory-mcp uninstall\n\n");

    cbm_detected_agents_t agents = cbm_detect_agents(home);
    if (agents.claude_code) {
        uninstall_claude_code(home, dry_run);
    }
    uninstall_cli_agents(&agents, home, dry_run);
    uninstall_editor_agents(&agents, home, dry_run);

    /* Step 2: Remove indexes */
    int index_count = count_db_indexes(home);
    if (index_count > 0) {
        printf("\nFound %d index(es):\n", index_count);
        cbm_list_indexes(home);
        if (prompt_yn("Delete these indexes?")) {
            int idx_removed = cbm_remove_indexes(home);
            printf("Removed %d index(es).\n", idx_removed);
        } else {
            printf("Indexes kept.\n");
        }
    }

    /* Step 3: Remove binary */
    char bin_path[CLI_BUF_1K];
#ifdef _WIN32
    snprintf(bin_path, sizeof(bin_path), "%s/.local/bin/codebase-memory-mcp.exe", home);
#else
    snprintf(bin_path, sizeof(bin_path), "%s/.local/bin/codebase-memory-mcp", home);
#endif
    struct stat st;
    if (stat(bin_path, &st) == 0) {
        if (!dry_run) {
            cbm_unlink(bin_path);
        }
        printf("Removed %s\n", bin_path);
    }

    printf("\nUninstall complete.\n");
    if (dry_run) {
        printf("(dry-run — no files were modified)\n");
    }
    return 0;
}

/* ── Subcommand: update ───────────────────────────────────────── */

/* Read archive from disk, extract binary (tar.gz or zip), write to bin_dest.
 * Returns 0 on success, 1 on failure. Cleans up tmp_archive. */

typedef struct {
    const char *tmp_archive;
    const char *ext;
    const char *bin_dest;
} extract_install_args_t;
static int extract_and_install_binary(extract_install_args_t args) {
    const char *tmp_archive = args.tmp_archive;
    const char *ext = args.ext;
    const char *bin_dest = args.bin_dest;
    FILE *f = fopen(tmp_archive, "rb");
    if (!f) {
        (void)fprintf(stderr, "error: cannot open %s\n", tmp_archive);
        return CLI_TRUE;
    }
    (void)fseek(f, 0, SEEK_END);
    long fsize = ftell(f);
    (void)fseek(f, 0, SEEK_SET);

    unsigned char *data = malloc((size_t)fsize);
    if (!data) {
        (void)fclose(f);
        cbm_unlink(tmp_archive);
        return CLI_TRUE;
    }
    (void)fread(data, CLI_ELEM_SIZE, (size_t)fsize, f);
    (void)fclose(f);

    int bin_len = 0;
    unsigned char *bin_data = NULL;
    if (strcmp(ext, "tar.gz") == 0) {
        bin_data = cbm_extract_binary_from_targz(data, (int)fsize, &bin_len);
    } else {
        bin_data = cbm_extract_binary_from_zip(data, (int)fsize, &bin_len);
    }
    free(data);
    cbm_unlink(tmp_archive);

    if (!bin_data || bin_len <= 0) {
        (void)fprintf(stderr, "error: binary not found in archive\n");
        free(bin_data);
        return CLI_TRUE;
    }

    if (cbm_replace_binary(bin_dest, bin_data, bin_len, CLI_OCTAL_PERM) != 0) {
        (void)fprintf(stderr, "error: cannot write to %s\n", bin_dest);
        free(bin_data);
        return CLI_TRUE;
    }
    free(bin_data);
    return 0;
}

/* Build the download URL for the update command. */
static void build_update_url(char *url, int url_sz, const char *os, const char *arch,
                             const char *ext, bool want_ui) {
    char base_url_buf[CLI_BUF_512];
    const char *base_url =
        cbm_safe_getenv("CBM_DOWNLOAD_URL", base_url_buf, sizeof(base_url_buf), NULL);
    if (!base_url || !base_url[0]) {
        base_url = "https://github.com/DeusData/codebase-memory-mcp/releases/latest/download";
    }
    snprintf(url, url_sz, "%s/codebase-memory-mcp-%s%s-%s.%s", base_url, want_ui ? "ui-" : "", os,
             arch, ext);
}

/* Prompt to delete existing indexes. Returns 0 to continue, 1 to abort. */
static int update_clear_indexes(const char *home, bool dry_run) {
    int index_count = count_db_indexes(home);
    if (index_count == 0) {
        return 0;
    }
    printf("Found %d existing index(es) that must be rebuilt after update:\n", index_count);
    cbm_list_indexes(home);
    printf("\n");
    if (dry_run) {
        printf("(dry-run — indexes would be deleted)\n\n");
        return 0;
    }
    if (!prompt_yn("Delete these indexes and continue with update?")) {
        printf("Update cancelled.\n");
        return CLI_TRUE;
    }
    int removed = cbm_remove_indexes(home);
    printf("Removed %d index(es).\n\n", removed);
    return 0;
}

/* Download, verify checksum, kill old instances, and install binary. Returns 0 on success. */
static int download_verify_install(const char *url, const char *ext, const char *os,
                                   const char *arch, bool want_ui, const char *bin_dest) {
    char tmp_archive[CLI_BUF_256];
    snprintf(tmp_archive, sizeof(tmp_archive), "%s/cbm-update.%s", cbm_tmpdir(), ext);

    int rc = cbm_download_to_file(url, tmp_archive);
    if (rc != 0) {
        (void)fprintf(stderr, "error: download failed (exit %d)\n", rc);
        cbm_unlink(tmp_archive);
        return CLI_TRUE;
    }

    char archive_name[CLI_BUF_256];
    snprintf(archive_name, sizeof(archive_name), "codebase-memory-mcp-%s%s-%s.%s",
             want_ui ? "ui-" : "", os, arch, ext);
    int crc = verify_download_checksum(tmp_archive, archive_name);
    if (crc == CLI_TRUE) {
        cbm_unlink(tmp_archive);
        return CLI_TRUE;
    }

    int killed = cbm_kill_other_instances();
    if (killed > 0) {
        printf("Stopped %d running MCP server instance(s).\n", killed);
    }

    if (extract_and_install_binary((extract_install_args_t){tmp_archive, ext, bin_dest}) != 0) {
        return CLI_TRUE;
    }
    return 0;
}

/* Select update variant. Returns 0=standard, 1=ui, -1=error. */
static int select_update_variant(int variant_flag) {
    if (variant_flag == VARIANT_A) {
        return 0;
    }
    if (variant_flag == VARIANT_B) {
        return CLI_TRUE;
    }
#ifndef _WIN32
    if (!isatty(fileno(stdin))) {
        (void)fprintf(stderr, "error: variant selection requires a terminal. "
                              "Use --standard or --ui flag.\n");
        return CLI_ERR;
    }
#endif
    printf("Which binary variant do you want?\n");
    printf("  1) standard  — MCP server only\n");
    printf("  2) ui        — MCP server + embedded graph visualization\n");
    printf("Choose (1/2): ");
    (void)fflush(stdout);
    char choice[CLI_BUF_16];
    if (!fgets(choice, sizeof(choice), stdin)) {
        (void)fprintf(stderr, "error: failed to read input\n");
        return CLI_ERR;
    }
    return (choice[0] == '2') ? CLI_TRUE : 0;
}

/* Case-insensitive prefix match (portable — no strncasecmp dependency). */
static bool prefix_icase(const char *s, const char *prefix) {
    while (*prefix) {
        if (tolower((unsigned char)*s) != tolower((unsigned char)*prefix)) {
            return false;
        }
        s++;
        prefix++;
    }
    return true;
}

/* Fetch latest release tag from GitHub via redirect header.
 * Returns heap-allocated tag (e.g. "v0.5.7") or NULL on failure. */
static char *fetch_latest_tag(void) {
    FILE *fp = cbm_popen(
        "curl -sfI https://github.com/DeusData/codebase-memory-mcp/releases/latest 2>/dev/null",
        "r");
    if (!fp) {
        return NULL;
    }
    char line[CBM_SZ_512];
    char *tag = NULL;
    while (fgets(line, sizeof(line), fp)) {
        if (!prefix_icase(line, "location:")) {
            continue;
        }
        char *slash = strrchr(line, '/');
        if (!slash) {
            break;
        }
        slash++;
        size_t len = strlen(slash);
        while (len > 0 && (slash[len - SKIP_ONE] == '\r' || slash[len - SKIP_ONE] == '\n' ||
                           slash[len - SKIP_ONE] == ' ')) {
            slash[--len] = '\0';
        }
        if (len > 0) {
            tag = strdup(slash);
        }
        break;
    }
    cbm_pclose(fp);
    return tag;
}

/* Check if current version is already latest. Returns true to skip update. */
static bool check_already_latest(void) {
    char dl_env[CBM_SZ_256] = "";
    cbm_safe_getenv("CBM_DOWNLOAD_URL", dl_env, sizeof(dl_env), NULL);
    if (dl_env[0]) {
        return false; /* testing override — always update */
    }
    char *latest = fetch_latest_tag();
    if (!latest) {
        (void)fprintf(stderr, "warning: could not check latest version (network unavailable?). "
                              "Proceeding with update.\n");
        return false;
    }
    int cmp = cbm_compare_versions(latest, CBM_VERSION);
    if (cmp <= 0) {
        if (cmp < 0) {
            printf("Already up to date (%s, ahead of latest %s).\n", CBM_VERSION, latest);
        } else {
            printf("Already up to date (%s).\n", CBM_VERSION);
        }
        free(latest);
        return true;
    }
    printf("Update available: %s -> %s\n", CBM_VERSION, latest);
    free(latest);
    return false;
}

int cbm_cmd_update(int argc, char **argv) {
    parse_auto_answer(argc, argv);

    bool dry_run = false;
    bool force = false;
    int variant_flag = 0; /* 0 = ask, 1 = standard, 2 = ui */
    for (int i = 0; i < argc; i++) {
        if (strcmp(argv[i], "--dry-run") == 0) {
            dry_run = true;
        } else if (strcmp(argv[i], "--standard") == 0) {
            variant_flag = VARIANT_A;
        } else if (strcmp(argv[i], "--ui") == 0) {
            variant_flag = VARIANT_B;
        } else if (strcmp(argv[i], "--force") == 0) {
            force = true;
        }
    }

    const char *home = cbm_get_home_dir();
    if (!home) {
        (void)fprintf(stderr, "error: HOME not set (use USERPROFILE on Windows)\n");
        return CLI_TRUE;
    }

    printf("codebase-memory-mcp update (current: %s)\n\n", CBM_VERSION);

    /* Version check — skip download if already on latest (not in dry-run). */
    if (!force && !dry_run && check_already_latest()) {
        return 0;
    }

    /* Step 1: Check for existing indexes */
    if (update_clear_indexes(home, dry_run) != 0) {
        return CLI_TRUE;
    }

    /* Step 2: Determine variant */
    int want_ui_rc = select_update_variant(variant_flag);
    if (want_ui_rc < 0) {
        return CLI_TRUE;
    }
    bool want_ui = (want_ui_rc == CLI_TRUE);
    const char *variant = want_ui ? "ui-" : "";
    const char *variant_label = want_ui ? "ui" : "standard";

    const char *os = detect_os();
    const char *arch = detect_arch();
    const char *ext = strcmp(os, "windows") == 0 ? "zip" : "tar.gz";

    char url[CLI_BUF_512];
    build_update_url(url, sizeof(url), os, arch, ext, want_ui);

    if (dry_run) {
        printf("\nWould download %s binary for %s/%s ...\n", variant_label, os, arch);
    } else {
        printf("\nDownloading %s binary for %s/%s ...\n", variant_label, os, arch);
    }
    printf("  %s\n", url);

    if (dry_run) {
        printf("\n(dry-run — skipping download, extraction, and binary replacement)\n");
        printf("  target: %s/.local/bin/codebase-memory-mcp\n", home);
        printf("  variant: %s\n", variant_label);
        printf("  os/arch: %s/%s\n", os, arch);
        printf("\nUpdate dry-run complete.\n");
        (void)variant;
        return 0;
    }

    /* Step 4-5: Download, verify, and install binary */
    char bin_dest[CLI_BUF_1K];
#ifdef _WIN32
    snprintf(bin_dest, sizeof(bin_dest), "%s/.local/bin/codebase-memory-mcp.exe", home);
#else
    snprintf(bin_dest, sizeof(bin_dest), "%s/.local/bin/codebase-memory-mcp", home);
#endif
    char bin_dir[CLI_BUF_1K];
    snprintf(bin_dir, sizeof(bin_dir), "%s/.local/bin", home);
    cbm_mkdir_p(bin_dir, CLI_OCTAL_PERM);

    int rc = download_verify_install(url, ext, os, arch, want_ui, bin_dest);
    if (rc != 0) {
        return CLI_TRUE;
    }

    /* Step 5b: macOS ad-hoc signing (required for arm64, harmless for x86_64) */
#ifdef __APPLE__
    if (cbm_macos_adhoc_sign(bin_dest) != 0) {
        (void)fprintf(stderr,
                      "warning: ad-hoc signing failed — binary may not run on macOS arm64\n");
    }
#endif

    /* Step 6: Refresh all agent configs (skills, MCP entries, hooks) */
    printf("Refreshing agent configurations...\n");
    cbm_install_agent_configs(home, bin_dest, true, false);

    /* Step 7: Verify new version (exec directly, no shell interpretation) */
    printf("\nUpdate complete. Verifying:\n");
    {
        const char *ver_argv[] = {bin_dest, "--version", NULL};
        (void)cbm_exec_no_shell(ver_argv);
    }

    printf("\nAll project indexes were cleared. They will be rebuilt\n");
    printf("automatically when you next use the MCP server.\n");
    printf("\nPlease restart your MCP client to use the new binary.\n");
    (void)variant;
    return 0;
}
