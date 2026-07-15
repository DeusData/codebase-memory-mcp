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
#include "foundation/str_util.h"
#include "foundation/sha256.h"
#include "pagerank/pagerank.h"
#include "pipeline/pipeline.h"
#include "mcp/mcp.h" // cbm_mcp_tool_input_schema — CLI flag parser + per-tool --help

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
#include <libproc.h>
#include <mach-o/dyld.h>
#endif
#ifdef _WIN32
#include <tlhelp32.h>
#endif
#include "foundation/compat_fs.h"

#ifndef CBM_VERSION
#define CBM_VERSION "dev"
#endif
#include <errno.h>  // EEXIST
#include <fcntl.h>  // open, O_WRONLY, O_CREAT, O_TRUNC
#include <limits.h> // UINT_MAX
#include <stdint.h> // uintptr_t
#include <stdarg.h>
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
        if (cbm_file_exists(buf)) {
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
 * On Windows, executable-bit checks are not portable, so existence is enough. */
static bool is_executable(const char *path) {
#ifdef _WIN32
    return cbm_file_exists(path);
#else
    struct stat st;
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
#ifdef _WIN32
        /* On Windows executables carry an extension (PATHEXT). A CLI like
         * opencode is often installed as a .cmd / .ps1 / .exe shim (e.g. via
         * mise or npm), so the bare-name probe above misses it (#221). Try the
         * common executable extensions before moving to the next PATH entry. */
        static const char *const win_exts[] = {".exe", ".cmd", ".bat", ".ps1", NULL};
        for (int i = 0; win_exts[i]; i++) {
            snprintf(out, out_sz, "%s/%s%s", dir, name, win_exts[i]);
            if (is_executable(out)) {
                return true;
            }
        }
#endif
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
    for (int i = 0; i < NUM_PATHS; i++) {
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

/* Return true if two paths refer to the same on-disk file. Used to avoid
 * copying the running binary onto itself during install (cbm_copy_file would
 * truncate it, since it opens the destination "wb" before reading the source). */
static bool cbm_same_file(const char *a, const char *b) {
    struct stat sa;
    struct stat sb;
    if (stat(a, &sa) != 0 || stat(b, &sb) != 0) {
        return false;
    }
#ifdef _WIN32
    /* st_ino is unreliable on Windows; compare normalized path strings. */
    char na[CLI_BUF_1K];
    char nb[CLI_BUF_1K];
    snprintf(na, sizeof(na), "%s", a);
    snprintf(nb, sizeof(nb), "%s", b);
    cbm_normalize_path_sep(na);
    cbm_normalize_path_sep(nb);
    return strcmp(na, nb) == 0;
#else
    return sa.st_dev == sb.st_dev && sa.st_ino == sb.st_ino;
#endif
}

/* Copy the running binary into the canonical install target, preserving the
 * executable bit. When src and dst are the same on-disk file the copy is
 * skipped: cbm_copy_file opens dst "wb" before reading src, so copying a file
 * onto itself would truncate it to zero. Returns 0 on success or skip,
 * CLI_ERR on failure. Exposed (non-static) as the regression surface for the
 * `install --force` binary-swap bug (#472). */
int cbm_copy_binary_to_target(const char *src, const char *dst) {
    if (cbm_same_file(src, dst)) {
        return 0; /* already in place — nothing to copy */
    }
    if (cbm_copy_file(src, dst) != 0) {
        return CLI_ERR;
    }
#ifndef _WIN32
    (void)chmod(dst, CLI_OCTAL_PERM);
#endif
    return 0;
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
    if (cbm_file_exists(path)) {
        /* File exists — remove or rename it */
        if (cbm_unlink(path) != 0) {
#ifdef _WIN32
            /* Windows: can't unlink running .exe — rename aside */
            char old_path[CLI_BUF_1K];
            int old_len = snprintf(old_path, sizeof(old_path), "%s.old", path);
            if (old_len < 0 || (size_t)old_len >= sizeof(old_path)) {
                return CLI_ERR;
            }
            (void)cbm_unlink(old_path);
            if (cbm_move_file_no_replace(path, old_path) != 0) {
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
    "Graph tools return structured code results with lower token cost than broad grep.\n"
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
    "| Text search | `search_code(pattern=\"...\")` or Grep |\n"
    "\n"
    "## Exploration Workflow\n"
    "1. `search_graph(pattern=\"...\")` — auto-indexes the server CWD or explicit repo path when auto_index=true and under auto_index_limit\n"
    "2. `get_code(qualified_name=\"project.path.FuncName\")` — read one symbol's source\n"
    "3. `query_graph(query=\"MATCH ...\")` — use Cypher for multi-hop graph questions\n"
    "4. `_hidden_tools` — reveal classic tools such as list_projects and get_graph_schema if needed\n"
    "\n"
    "## Tracing Workflow\n"
    "1. `search_graph(name_pattern=\".*FuncName.*\")` — discover exact name\n"
    "2. `trace_path(function_name=\"FuncName\", direction=\"both\", depth=3)` — trace callers and callees\n"
    "3. `detect_changes()` — map git diff to affected symbols\n"
    "\n"
    "## Quality Analysis\n"
    "- Dead code: `search_graph(max_degree=0, exclude_entry_points=true)`\n"
    "- High fan-out: `query_graph(query=\"MATCH (f)-[:CALLS]->(g) RETURN f.name, count(g) AS out_degree ORDER BY out_degree DESC LIMIT 20\")`\n"
    "- High fan-in: `query_graph(query=\"MATCH (f)<-[:CALLS]-(g) RETURN f.name, count(g) AS in_degree ORDER BY in_degree DESC LIMIT 20\")`\n"
    "\n"
    "## Default MCP Tools\n"
    "`search_graph`, `query_graph`, `search_code`, `trace_path`, `get_code`\n"
    "\n"
    "Graph-backed default tools (`search_graph`, `query_graph`, `trace_path`, `get_code`)\n"
    "auto-index the server CWD or explicit repo path when auto_index=true and under\n"
    "auto_index_limit. `search_code` searches source files for an already indexed/current project.\n"
    "\n"
    "Use `_hidden_tools` to reveal advanced tools such as `index_repository`,\n"
    "`get_graph_schema`, `get_architecture`, `detect_changes`, and `index_dependencies`.\n"
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
    "2. `query_graph` output is capped by query_max_output_bytes; add LIMIT or set max_output_bytes=0.\n"
    "3. `trace_path` works best with exact names — use `search_graph(name_pattern=...)` first.\n"
    "4. `direction=\"outbound\"` returns callees only; use `direction=\"both\"` for callers too.\n"
    "5. Results default to search_limit (50 unless configured); check `has_more` and use `offset`.\n";

static const char codex_instructions_content[] =
    "# Codebase Knowledge Graph\n"
    "\n"
    "This project uses codebase-memory-mcp to maintain a knowledge graph of the codebase.\n"
    "Use the MCP tools to explore and understand the code:\n"
    "\n"
    "- `search_graph` — find functions, classes, routes by pattern; graph-backed tools can auto-index the server CWD or explicit repo path when auto_index=true and under auto_index_limit\n"
    "- `trace_path` — trace who calls a function or what it calls\n"
    "- `get_code` — read function source code by qualified_name\n"
    "- `query_graph` — run Cypher queries for complex patterns\n"
    "- `get_architecture` — high-level summary after `_hidden_tools` reveal or `CBM_TOOL_MODE=classic`\n"
    "\n"
    "Prefer graph tools over grep for structural code discovery.\n";

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

static bool cbm_snprintf_fits(int n, size_t out_sz) {
    return n >= 0 && (size_t)n < out_sz;
}

static bool cbm_format_fits(char *out, size_t out_sz, const char *fmt, ...) {
    if (!out || out_sz == 0 || !fmt) {
        return false;
    }
    va_list ap;
    va_start(ap, fmt);
    int n = vsnprintf(out, out_sz, fmt, ap);
    va_end(ap);
    if (!cbm_snprintf_fits(n, out_sz)) {
        out[0] = '\0';
        return false;
    }
    return true;
}

static int cbm_prepare_parent_dir(const char *path) {
    if (!path || !path[0]) {
        return CLI_ERR;
    }

    char dir[CBM_PATH_MAX];
    if (!cbm_format_fits(dir, sizeof(dir), "%s", path)) {
        return CLI_ERR;
    }

    char *last_slash = strrchr(dir, '/');
#ifdef _WIN32
    char *last_bslash = strrchr(dir, '\\');
    if (last_bslash && (!last_slash || last_bslash > last_slash)) {
        last_slash = last_bslash;
    }
#endif
    if (!last_slash) {
        return CLI_OK;
    }
    *last_slash = '\0';
    if (!dir[0]) {
        return CLI_OK;
    }
    return mkdirp(dir, DIR_PERMS);
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
    if (cbm_prepare_parent_dir(path) != CLI_OK) {
        return CLI_ERR;
    }

    yyjson_write_flag flags = YYJSON_WRITE_PRETTY | YYJSON_WRITE_ESCAPE_UNICODE;
    size_t len;
    char *json = yyjson_mut_write(doc, flags, &len);
    if (!json) {
        return CLI_ERR;
    }
    if (len > SIZE_MAX - (size_t)CLI_PAIR_LEN) {
        free(json);
        return CLI_ERR;
    }

    int rc = CLI_ERR;
    char *json_nl = malloc(len + CLI_PAIR_LEN);
    if (json_nl) {
        memcpy(json_nl, json, len);
        json_nl[len] = '\n';
        json_nl[len + CLI_SKIP_ONE] = '\0';
        int wrc = cbm_write_file_atomic(path, json_nl, len + CLI_SKIP_ONE, NULL);
        rc = wrc == 0 ? CLI_OK : CLI_ERR;
        free(json_nl);
    }
    free(json);

    return rc;
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

/* ── OpenClaw MCP (nested mcp.servers with command + args) ────── */

int cbm_install_openclaw_mcp(const char *binary_path, const char *config_path) {
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

    yyjson_mut_val *mcp = yyjson_mut_obj_get(root, "mcp");
    if (!mcp || !yyjson_mut_is_obj(mcp)) {
        mcp = yyjson_mut_obj(mdoc);
        yyjson_mut_obj_add_val(mdoc, root, "mcp", mcp);
    }

    yyjson_mut_val *servers = yyjson_mut_obj_get(mcp, "servers");
    if (!servers || !yyjson_mut_is_obj(servers)) {
        servers = yyjson_mut_obj(mdoc);
        yyjson_mut_obj_add_val(mdoc, mcp, "servers", servers);
    }

    yyjson_mut_obj_remove_key(servers, "codebase-memory-mcp");

    yyjson_mut_val *entry = yyjson_mut_obj(mdoc);
    yyjson_mut_obj_add_bool(mdoc, entry, "enabled", true);
    yyjson_mut_obj_add_str(mdoc, entry, "command", binary_path);
    yyjson_mut_val *args = yyjson_mut_arr(mdoc);
    yyjson_mut_obj_add_val(mdoc, entry, "args", args);
    yyjson_mut_obj_add_val(mdoc, servers, "codebase-memory-mcp", entry);

    int rc = write_json_file(config_path, mdoc);
    yyjson_mut_doc_free(mdoc);
    return rc;
}

int cbm_remove_openclaw_mcp(const char *config_path) {
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

    yyjson_mut_val *servers = yyjson_mut_obj_get(mcp, "servers");
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
    return cbm_is_dir(path);
}

/* Resolve the Claude Code config dir.
 * Honors $CLAUDE_CONFIG_DIR; falls back to "$home_dir/.claude". */
static void cbm_claude_config_dir(const char *home_dir, char *out, size_t out_sz) {
    if (out_sz == 0) {
        return;
    }
    out[0] = '\0';
    char env_buf[CLI_BUF_1K];
    bool env_present = false;
    const char *env = cbm_getenv_fits("CLAUDE_CONFIG_DIR", env_buf, sizeof(env_buf),
                                      &env_present)
                          ? env_buf
                          : NULL;
    if (env && env[0]) {
        (void)cbm_format_fits(out, out_sz, "%s", env);
    } else if (env_present) {
        return;
    } else if (home_dir && home_dir[0]) {
        (void)cbm_format_fits(out, out_sz, "%s/.claude", home_dir);
    }
}

/* Resolve the parent dir containing `.claude.json` (Claude Code's user config file).
 * Honors $CLAUDE_CONFIG_DIR; falls back to "$home_dir". */
static void cbm_claude_user_root(const char *home_dir, char *out, size_t out_sz) {
    if (out_sz == 0) {
        return;
    }
    out[0] = '\0';
    char env_buf[CLI_BUF_1K];
    bool env_present = false;
    const char *env = cbm_getenv_fits("CLAUDE_CONFIG_DIR", env_buf, sizeof(env_buf),
                                      &env_present)
                          ? env_buf
                          : NULL;
    if (env && env[0]) {
        (void)cbm_format_fits(out, out_sz, "%s", env);
    } else if (env_present) {
        return;
    } else if (home_dir && home_dir[0]) {
        (void)cbm_format_fits(out, out_sz, "%s", home_dir);
    }
}

/* Build the hook command string written into Claude Code's settings.json.
 * Honors $CLAUDE_CONFIG_DIR. When CLAUDE_CONFIG_DIR is unset, preserves the
 * legacy tilde-expanded form so settings.json stays portable across HOME values. */
static bool cbm_resolve_hook_command(const char *script_name, char *out, size_t out_sz) {
    if (out_sz == 0) {
        return false;
    }
    out[0] = '\0';
    char env_buf[CLI_BUF_1K];
    bool env_present = false;
    const char *env = cbm_getenv_fits("CLAUDE_CONFIG_DIR", env_buf, sizeof(env_buf),
                                      &env_present)
                          ? env_buf
                          : NULL;
    if (env && env[0]) {
        return cbm_format_fits(out, out_sz, "%s/hooks/%s", env, script_name);
    } else if (!env_present) {
        return cbm_format_fits(out, out_sz, "~/.claude/hooks/%s", script_name);
    }
    return false;
}

cbm_detected_agents_t cbm_detect_agents(const char *home_dir) {
    cbm_detected_agents_t agents;
    memset(&agents, 0, sizeof(agents));
    if (!home_dir || !home_dir[0]) {
        return agents;
    }

    char path[CLI_BUF_1K];

    cbm_claude_config_dir(home_dir, path, sizeof(path));
    agents.claude_code = path[0] != '\0' && dir_exists(path);

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

    /* Antigravity CLI (2026 unification) installs under ~/.gemini/antigravity-cli/
     * (brain/, mcp/, settings.json), with MCP config in the shared
     * ~/.gemini/config/mcp_config.json. */
    snprintf(path, sizeof(path), "%s/.gemini/antigravity-cli", home_dir);
    if (dir_exists(path)) {
        agents.antigravity = true;
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

    /* Cursor stores its user MCP config in ~/.cursor/mcp.json on all platforms. */
    snprintf(path, sizeof(path), "%s/.cursor", home_dir);
    agents.cursor = dir_exists(path);

    snprintf(path, sizeof(path), "%s/.openclaw", home_dir);
    agents.openclaw = dir_exists(path);

    /* Kiro: ~/.kiro/ */
    snprintf(path, sizeof(path), "%s/.kiro", home_dir);
    agents.kiro = dir_exists(path);

    /* Junie (JetBrains): ~/.junie/ */
    snprintf(path, sizeof(path), "%s/.junie", home_dir);
    agents.junie = dir_exists(path);

    return agents;
}

/* ── Shared agent instructions content ────────────────────────── */

static const char agent_instructions_content[] =
    "# Codebase Knowledge Graph (codebase-memory-mcp)\n"
    "\n"
    "This project uses codebase-memory-mcp to maintain a knowledge graph of the codebase.\n"
    "Prefer MCP graph tools over grep/glob/file-search for structural code discovery.\n"
    "\n"
    "## Priority Order\n"
    "1. `search_graph` — find functions, classes, routes, variables by pattern\n"
    "2. `trace_path` — trace who calls a function or what it calls\n"
    "3. `get_code` — read specific function/class source code by qualified_name\n"
    "4. `query_graph` — run Cypher queries for complex patterns\n"
    "5. `get_architecture` — high-level summary after `_hidden_tools` reveal or `CBM_TOOL_MODE=classic`\n"
    "\n"
    "## When to fall back to grep/glob\n"
    "- Searching for string literals, error messages, config values\n"
    "- Searching non-code files (Dockerfiles, shell scripts, configs)\n"
    "- When MCP tools return insufficient results\n"
    "\n"
    "## Examples\n"
    "- Find a handler: `search_graph(name_pattern=\".*OrderHandler.*\")`\n"
    "- Who calls it: `trace_path(function_name=\"OrderHandler\", direction=\"inbound\")`\n"
    "- Read source: `get_code(qualified_name=\"pkg/orders.OrderHandler\")`\n";

/* #1032: Aider has NO MCP support — it reads CONVENTIONS.md but can only run
 * shell commands. Installing the MCP-tool-centric instructions above told the
 * model to call tools it cannot invoke. Aider gets a CLI-form variant: the
 * exact same discovery priority, expressed as runnable `codebase-memory-mcp
 * cli` commands (usable via Aider's /run or auto-approved shell). */
static const char aider_instructions_content[] =
    "# Codebase Knowledge Graph (codebase-memory-mcp)\n"
    "\n"
    "This project uses codebase-memory-mcp to maintain a knowledge graph of the codebase.\n"
    "Aider has no MCP support, so invoke the graph through the CLI (e.g. via /run).\n"
    "ALWAYS prefer these commands over grep/glob/file-search for code discovery.\n"
    "\n"
    "## Priority Order (CLI form)\n"
    "1. Find functions/classes/routes:\n"
    "   codebase-memory-mcp cli search_graph "
    "'{\"project\":\"<name>\",\"name_pattern\":\".*Foo.*\"}'\n"
    "2. Who calls X / what does X call:\n"
    "   codebase-memory-mcp cli trace_path "
    "'{\"project\":\"<name>\",\"function_name\":\"Foo\",\"direction\":\"both\"}'\n"
    "3. Read a specific function/class:\n"
    "   codebase-memory-mcp cli get_code_snippet "
    "'{\"project\":\"<name>\",\"qualified_name\":\"<qn>\"}'\n"
    "4. Complex patterns (Cypher):\n"
    "   codebase-memory-mcp cli query_graph '{\"project\":\"<name>\",\"query\":\"MATCH ...\"}'\n"
    "5. Project overview:\n"
    "   codebase-memory-mcp cli get_architecture '{\"project\":\"<name>\"}'\n"
    "\n"
    "First use in a repo: codebase-memory-mcp cli index_repository '{\"repo_path\":\"<abs "
    "path>\"}'\n"
    "List indexed projects (for <name>): codebase-memory-mcp cli list_projects '{}'\n"
    "\n"
    "## When to fall back to grep/glob\n"
    "- Searching for string literals, error messages, config values\n"
    "- Searching non-code files (Dockerfiles, shell scripts, configs)\n"
    "- When the CLI returns insufficient results\n";

const char *cbm_get_aider_instructions(void) {
    return aider_instructions_content;
}

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
    if (cbm_prepare_parent_dir(path) != CLI_OK) {
        return CLI_ERR;
    }

    size_t len = strlen(content);
    return cbm_write_file_atomic(path, content, len, NULL) == 0 ? CLI_OK : CLI_ERR;
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
#define CODEX_HOOK_BEGIN "# >>> codebase-memory-mcp SessionStart >>>"
#define CODEX_HOOK_END "# <<< codebase-memory-mcp SessionStart <<<"

static const char *codex_mcp_section_suffix(const char *section_end) {
    const char *next_section = strstr(section_end, "\n[");
    const char *hook_begin = strstr(section_end, CODEX_HOOK_BEGIN);
    if (hook_begin && (!next_section || hook_begin < next_section)) {
        return hook_begin;
    }
    return next_section ? next_section + CLI_SKIP_ONE : "";
}

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
        const char *suffix = codex_mcp_section_suffix(section_end);

        size_t prefix_len = (size_t)(existing - content);
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
    const char *suffix = codex_mcp_section_suffix(section_end);

    /* Remove leading newline if present */
    if (existing > content && *(existing - CLI_SKIP_ONE) == '\n') {
        existing--;
    }

    size_t prefix_len = (size_t)(existing - content);
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

/* ── SessionStart reminder hook (Codex / Gemini / Antigravity) ──────
 * Non-blocking lifecycle hook whose stdout is injected as session context,
 * reminding the agent to use codebase-memory-mcp graph tools first. This
 * stdout is harness context, not MCP protocol output. The command is written
 * so it is valid both inside a TOML single-quoted literal (Codex config.toml)
 * and a JSON string (Gemini settings.json) — i.e. it contains NO single quotes
 * and NO newlines. (issues #330 + Gemini/Antigravity parity) */
#define CMM_SESSION_REMINDER_CMD                                                    \
    "echo \"Code discovery: prefer codebase-memory-mcp (search_graph, trace_path, " \
    "get_code, query_graph, search_code) before broad grep for structural code "    \
    "discovery; graph-backed tools auto-index the MCP server CWD or explicit repo " \
    "paths when auto_index=true and under "                                        \
    "auto_index_limit; search_code needs an indexed project; call _hidden_tools "   \
    "for explicit index_repository.\""

/* Sentinel-delimited block so upsert/remove are robust to the nested TOML
 * array-of-tables (which both start with '['). */
static const char *codex_find_session_start_table(const char *from, const char *end_marker) {
    const char *line = from;
    const char *last = NULL;
    while (line && line < end_marker) {
        if (strncmp(line, "[[hooks.SessionStart]]", SLEN("[[hooks.SessionStart]]")) == 0) {
            last = line;
        }
        const char *next = strchr(line, '\n');
        if (!next || next >= end_marker) {
            break;
        }
        line = next + CLI_SKIP_ONE;
    }
    return last;
}

static bool codex_hook_block_bounds(const char *from, const char **out_begin,
                                    const char **out_end) {
    const char *begin = strstr(from, CODEX_HOOK_BEGIN);
    const char *end_marker = strstr(from, CODEX_HOOK_END);
    if (!begin && !end_marker) {
        return false;
    }

    const char *block_begin = NULL;
    if (begin && (!end_marker || begin < end_marker)) {
        block_begin = begin;
        end_marker = strstr(begin, CODEX_HOOK_END);
    } else {
        block_begin = codex_find_session_start_table(from, end_marker);
    }
    if (!block_begin || !end_marker) {
        return false;
    }

    const char *block_end = end_marker + strlen(CODEX_HOOK_END);
    if (*block_end == '\n') {
        block_end++;
    }
    if (block_begin > from && *(block_begin - CLI_SKIP_ONE) == '\n') {
        block_begin--;
    }
    *out_begin = block_begin;
    *out_end = block_end;
    return true;
}

/* Splice out all CMM Codex SessionStart hook blocks. Returns a newly-malloc'd
 * string the caller frees, or NULL if no block was present. */
static char *codex_hook_strip(const char *content) {
    size_t content_len = strlen(content);
    char *out = malloc(content_len + CLI_SKIP_ONE);
    if (!out) {
        return NULL;
    }

    const char *cursor = content;
    size_t out_len = 0;
    bool changed = false;
    const char *begin;
    const char *end;
    while (codex_hook_block_bounds(cursor, &begin, &end)) {
        size_t keep_len = (size_t)(begin - cursor);
        memcpy(out + out_len, cursor, keep_len);
        out_len += keep_len;
        cursor = end;
        changed = true;
    }
    if (!changed) {
        free(out);
        return NULL;
    }

    size_t suffix_len = strlen(cursor);
    memcpy(out + out_len, cursor, suffix_len);
    out[out_len + suffix_len] = '\0';
    return out;
}

/* Install/update the Codex SessionStart reminder hook in config.toml. */
int cbm_upsert_codex_hooks(const char *config_path) {
    if (!config_path) {
        return CLI_ERR;
    }
    char block[CLI_BUF_2K];
    snprintf(block, sizeof(block),
             "\n" CODEX_HOOK_BEGIN "\n"
             "[[hooks.SessionStart]]\n"
             "matcher = \"startup|resume|clear|compact\"\n\n"
             "[[hooks.SessionStart.hooks]]\n"
             "type = \"command\"\n"
             "command = '%s'\n" CODEX_HOOK_END "\n",
             CMM_SESSION_REMINDER_CMD);

    size_t len = 0;
    char *content = read_file_str(config_path, &len);
    if (!content) {
        return write_file_str(config_path, block + CLI_SKIP_ONE); /* skip leading newline */
    }
    char *stripped = codex_hook_strip(content);
    const char *base = stripped ? stripped : content;
    size_t base_len = strlen(base);
    char *result = malloc(base_len + strlen(block) + CLI_SKIP_ONE);
    if (!result) {
        free(content);
        free(stripped);
        return CLI_ERR;
    }
    memcpy(result, base, base_len);
    memcpy(result + base_len, block, strlen(block));
    result[base_len + strlen(block)] = '\0';
    int rc = write_file_str(config_path, result);
    free(content);
    free(stripped);
    free(result);
    return rc;
}

int cbm_remove_codex_hooks(const char *config_path) {
    if (!config_path) {
        return CLI_ERR;
    }
    size_t len = 0;
    char *content = read_file_str(config_path, &len);
    if (!content) {
        return CLI_TRUE;
    }
    char *stripped = codex_hook_strip(content);
    if (!stripped) {
        free(content);
        return CLI_TRUE; /* nothing to remove */
    }
    int rc = write_file_str(config_path, stripped);
    free(content);
    free(stripped);
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

/* ── Junie MCP config (JSON, same mcpServers format) ──────────── */

int cbm_upsert_junie_mcp(const char *binary_path, const char *config_path) {
    /* Junie (JetBrains) uses same mcpServers format as Cursor/Antigravity */
    return cbm_install_editor_mcp(binary_path, config_path);
}

int cbm_remove_junie_mcp(const char *config_path) {
    return cbm_remove_editor_mcp(config_path);
}

/* ── Claude Code pre-tool hooks ───────────────────────────────── */

/* Matcher includes Read for the indexing-coverage note (#963): when the agent
 * reads a file the indexer could not fully cover, the hook injects a warning
 * as additionalContext. The issue-#362 hazard (a GATING hook denying Read and
 * breaking the read-before-edit invariant) cannot recur: the augmenter is
 * structurally non-blocking — it always exits 0 and only ever ADDS context —
 * mirroring the Gemini matcher, which already includes read_file. */
#define CMM_HOOK_MATCHER "Grep|Glob|Read"
/* Basename only; the full command path is resolved at install time via
 * cbm_resolve_hook_command so $CLAUDE_CONFIG_DIR is honored. */
#ifdef _WIN32
/* #929: extensionless bash shims under %USERPROFILE%\\.claude\\hooks trigger
 * the "How do you want to open this file?" dialog when editors (Cursor) scan
 * the hooks dir, and cannot execute without bash anyway. Windows installs
 * .cmd scripts; the extensionless legacy files are removed on upgrade. */
#define CMM_HOOK_GATE_SCRIPT "cbm-code-discovery-gate.cmd"
#else
#define CMM_HOOK_GATE_SCRIPT "cbm-code-discovery-gate"
#endif
#define CMM_HOOK_GATE_SCRIPT_LEGACY "cbm-code-discovery-gate"
/* Hard backstop in settings.json; the binary also self-bounds with an
 * in-process deadline well under this. */
#define CMM_HOOK_TIMEOUT_SEC 5

/* Old matcher values from previous versions — recognized during upgrade so
 * upsert/remove can clean them up before inserting the current matcher.
 * Per-agent lists (no shared global): each caller passes its own. */
static const char *const cmm_claude_old_matchers[] = {
    "Grep|Glob|Read|Search",
    "Grep|Glob", /* pre-#963 matcher — Read re-added for the coverage note */
    NULL,
};
static const char *const cmm_gemini_old_matchers[] = {
    "google_search|read_file|grep_search",
    NULL,
};

/* Check if a hook array entry is ours (current matcher or a known old one).
 * When require_command_substr is non-NULL, the matcher match is not sufficient:
 * the entry must ALSO carry a hooks[].command containing that substring. This
 * disambiguates our entry from a user's own hook that happens to share the same
 * matcher (notably "*", which a user is likely to pick for a catch-all hook), so
 * upsert/remove never clobber a foreign entry. NULL preserves matcher-only
 * matching for callers whose matcher is already CMM-specific (e.g. "startup"). */
static bool is_cmm_hook_entry(yyjson_mut_val *entry, const char *matcher_str,
                              const char *const *old_matchers, const char *require_command_substr) {
    yyjson_mut_val *matcher = yyjson_mut_obj_get(entry, "matcher");
    if (!matcher || !yyjson_mut_is_str(matcher)) {
        return false;
    }
    const char *val = yyjson_mut_get_str(matcher);
    if (!val) {
        return false;
    }
    bool matcher_ok = strcmp(val, matcher_str) == 0;
    /* Also match old versions for backwards-compatible upgrade */
    for (int i = 0; !matcher_ok && old_matchers && old_matchers[i]; i++) {
        if (strcmp(val, old_matchers[i]) == 0) {
            matcher_ok = true;
        }
    }
    if (!matcher_ok) {
        return false;
    }
    if (require_command_substr) {
        yyjson_mut_val *hooks = yyjson_mut_obj_get(entry, "hooks");
        if (!hooks || !yyjson_mut_is_arr(hooks)) {
            return false;
        }
        size_t idx;
        size_t max;
        yyjson_mut_val *h;
        yyjson_mut_arr_foreach(hooks, idx, max, h) {
            yyjson_mut_val *cmd = yyjson_mut_obj_get(h, "command");
            if (cmd && yyjson_mut_is_str(cmd)) {
                const char *cs = yyjson_mut_get_str(cmd);
                if (cs && strstr(cs, require_command_substr)) {
                    return true;
                }
            }
        }
        return false;
    }
    return true;
}

/* Generic hook upsert for both Claude Code and Gemini CLI */

typedef struct {
    const char *settings_path;
    const char *hook_event;
    const char *matcher_str;
    const char *command_str;
    const char *const *old_matchers;  /* NULL-terminated; may be NULL */
    int timeout_sec;                  /* >0 adds "timeout" to the hook entry */
    const char *match_command_substr; /* non-NULL: also require this in the
                                       * entry command to claim ownership */
} hooks_upsert_args_t;
static int upsert_hooks_json(hooks_upsert_args_t args) {
    const char *settings_path = args.settings_path;
    const char *hook_event = args.hook_event;
    const char *matcher_str = args.matcher_str;
    const char *command_str = args.command_str;
    const char *const *old_matchers = args.old_matchers;
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
        if (is_cmm_hook_entry(item, matcher_str, old_matchers, args.match_command_substr)) {
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
    if (args.timeout_sec > 0) {
        yyjson_mut_obj_add_int(mdoc, hook_obj, "timeout", args.timeout_sec);
    }
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
    const char *const *old_matchers;  /* NULL-terminated; may be NULL */
    const char *match_command_substr; /* non-NULL: also require this in the
                                       * entry command to claim ownership */
} hooks_remove_args_t;
static int remove_hooks_json(hooks_remove_args_t args) {
    const char *settings_path = args.settings_path;
    const char *hook_event = args.hook_event;
    const char *matcher_str = args.matcher_str;
    const char *const *old_matchers = args.old_matchers;
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
        if (is_cmm_hook_entry(item, matcher_str, old_matchers, args.match_command_substr)) {
            yyjson_mut_arr_remove(event_arr, idx);
            break;
        }
    }

    /* Prune the event key once its array is empty, so removing our hook leaves
     * no stale "<Event>": [] cruft behind. */
    if (yyjson_mut_arr_size(event_arr) == 0) {
        yyjson_mut_obj_remove_key(hooks, hook_event);
    }

    int rc = write_json_file(settings_path, mdoc);
    yyjson_mut_doc_free(mdoc);
    return rc;
}

int cbm_upsert_claude_hooks(const char *settings_path) {
    char command[CLI_BUF_1K];
    if (!cbm_resolve_hook_command(CMM_HOOK_GATE_SCRIPT, command, sizeof(command))) {
        return CLI_ERR;
    }
    return upsert_hooks_json((hooks_upsert_args_t){
        .settings_path = settings_path,
        .hook_event = "PreToolUse",
        .matcher_str = CMM_HOOK_MATCHER,
        .command_str = command,
        .old_matchers = cmm_claude_old_matchers,
        .timeout_sec = CMM_HOOK_TIMEOUT_SEC,
    });
}

int cbm_remove_claude_hooks(const char *settings_path) {
    return remove_hooks_json((hooks_remove_args_t){
        .settings_path = settings_path,
        .hook_event = "PreToolUse",
        .matcher_str = CMM_HOOK_MATCHER,
        .old_matchers = cmm_claude_old_matchers,
    });
}

/* Install the search-augmenter shim to ~/.claude/hooks/.
 * The shim is a thin wrapper that delegates to `<binary> hook-augment`,
 * which adds graph context to Grep/Glob calls. It NEVER blocks a tool call:
 * a missing/old/hung binary results in a silent exit 0 (issue #362/#288).
 * The legacy filename `cbm-code-discovery-gate` is retained so existing
 * settings.json entries and uninstall keep working with zero migration. */
/* #929 (Windows): remove the pre-.cmd extensionless twin so upgrades stop
 * triggering the Open-With dialog. POSIX keeps the extensionless name, where
 * legacy == current — never unlink there. */
static void cbm_remove_legacy_hook_script(const char *hooks_dir, const char *legacy_name) {
#ifdef _WIN32
    char legacy_path[CLI_BUF_1K];
    snprintf(legacy_path, sizeof(legacy_path), "%s/%s", hooks_dir, legacy_name);
    cbm_unlink(legacy_path);
#else
    (void)hooks_dir;
    (void)legacy_name;
#endif
}

void cbm_install_hook_gate_script(const char *home, const char *binary_path) {
    if (!home || !binary_path) {
        return;
    }
    /* Defensive: refuse to embed a binary path containing a double-quote, which
     * would break the BIN="..." shell quoting in the generated shim. In normal
     * installs this is unreachable (paths come from cbm_detect_self_path), but
     * fail-loud here beats silently emitting a malformed script. */
    if (strchr(binary_path, '"') != NULL) {
        return;
    }
    char config_dir[CLI_BUF_1K];
    cbm_claude_config_dir(home, config_dir, sizeof(config_dir));
    if (!config_dir[0]) {
        return;
    }
    char hooks_dir[CLI_BUF_1K];
    if (!cbm_format_fits(hooks_dir, sizeof(hooks_dir), "%s/hooks", config_dir) ||
        !cbm_mkdir_p(hooks_dir, CLI_OCTAL_PERM)) {
        return;
    }

    cbm_remove_legacy_hook_script(hooks_dir, CMM_HOOK_GATE_SCRIPT_LEGACY);
    char script_path[CLI_BUF_1K];
    if (!cbm_format_fits(script_path, sizeof(script_path), "%s/" CMM_HOOK_GATE_SCRIPT,
                         hooks_dir)) {
        return;
    }

    FILE *f = fopen(script_path, "w");
    if (!f) {
        return;
    }
#ifdef _WIN32
    (void)fprintf(f,
                  "@echo off\r\n"
                  "REM codebase-memory-mcp search augmenter (Claude Code PreToolUse).\r\n"
                  "REM Never blocks a tool call - it only adds graph context.\r\n"
                  "REM Any failure is silent (exit 0, no output).\r\n"
                  "if not exist \"%s\" exit /b 0\r\n"
                  "\"%s\" hook-augment 2>NUL\r\n"
                  "exit /b 0\r\n",
                  binary_path, binary_path);
#else
    (void)fprintf(f,
                  "#!/usr/bin/env bash\n"
                  "# codebase-memory-mcp search augmenter (Claude Code PreToolUse).\n"
                  "# NOTE: the legacy filename is kept for zero-migration upgrades.\n"
                  "# Despite the name this NEVER blocks a tool call - it only adds\n"
                  "# graph context. Any failure is silent (exit 0, no output).\n"
                  "BIN=\"%s\"\n"
                  "[ -x \"$BIN\" ] || exit 0\n"
                  "\"$BIN\" hook-augment 2>/dev/null\n"
                  "exit 0\n",
                  binary_path);
#endif
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
#ifdef _WIN32
#define CMM_SESSION_REMINDER_SCRIPT "cbm-session-reminder.cmd"
#else
#define CMM_SESSION_REMINDER_SCRIPT "cbm-session-reminder"
#endif
#define CMM_SESSION_REMINDER_SCRIPT_LEGACY "cbm-session-reminder"

static void cbm_install_session_reminder_script(const char *home) {
    if (!home) {
        return;
    }
    char config_dir[CLI_BUF_1K];
    cbm_claude_config_dir(home, config_dir, sizeof(config_dir));
    if (!config_dir[0]) {
        return;
    }
    char hooks_dir[CLI_BUF_1K];
    if (!cbm_format_fits(hooks_dir, sizeof(hooks_dir), "%s/hooks", config_dir) ||
        !cbm_mkdir_p(hooks_dir, CLI_OCTAL_PERM)) {
        return;
    }

    cbm_remove_legacy_hook_script(hooks_dir, CMM_SESSION_REMINDER_SCRIPT_LEGACY);
    char script_path[CLI_BUF_1K];
    if (!cbm_format_fits(script_path, sizeof(script_path), "%s/" CMM_SESSION_REMINDER_SCRIPT,
                         hooks_dir)) {
        return;
    }

    FILE *f = fopen(script_path, "w");
    if (!f) {
        return;
    }
#ifdef _WIN32
    /* cmd variant: echo per line; `|` must be caret-escaped in cmd. */
    (void)fprintf(
        f,
        "@echo off\r\n"
        "REM SessionStart hook: remind agent to use codebase-memory-mcp tools.\r\n"
        "echo CRITICAL - Code Discovery Protocol:\r\n"
        "echo 1. ALWAYS use codebase-memory-mcp tools FIRST for ANY code exploration:\r\n"
        "echo    - search_graph(name_pattern/label/qn_pattern) to find functions/classes/routes\r\n"
        "echo    - trace_path(function_name, mode=calls^|data_flow^|cross_service) for call "
        "chains\r\n"
        "echo    - get_code_snippet(qualified_name) for exact symbol source (precise ranges)\r\n"
        "echo    - query_graph(query) for complex Cypher patterns\r\n"
        "echo    - get_architecture(aspects) for project structure\r\n"
        "echo    - search_code(pattern) for text search (graph-augmented grep)\r\n"
        "echo 2. Use Grep/Glob/Read freely for text, configs, non-code files, and\r\n"
        "echo    always Read a file before editing it.\r\n"
        "echo 3. If a project is not indexed yet, run index_repository FIRST.\r\n");
#else
    (void)fprintf(
        f, "#!/usr/bin/env bash\n"
           "# SessionStart hook: remind agent to use codebase-memory-mcp tools.\n"
           "# Installed by codebase-memory-mcp. Fires on startup/resume/clear/compact.\n"
           "cat << 'REMINDER'\n"
           "Code Discovery Protocol:\n"
           "1. Prefer codebase-memory-mcp tools first for structural code exploration:\n"
           "   - search_graph(name_pattern/label/qn_pattern) to find functions/classes/routes\n"
           "   - trace_path(function_name, mode=calls|data_flow|cross_service) for call chains\n"
           "   - get_code(qualified_name) for exact symbol source in streamlined mode\n"
           "   - query_graph(query) for complex Cypher patterns\n"
           "   - search_code(pattern) for text/regex source search in an indexed project\n"
           "2. Use Grep/Glob/Read freely for text, configs, non-code files, and\n"
           "   always Read a file before editing it.\n"
           "3. Graph-backed tools auto-index the server CWD or explicit repo paths when\n"
           "   auto_index=true and under auto_index_limit. search_code needs an\n"
           "   indexed project. Use _hidden_tools\n"
           "   to reveal index_repository or get_architecture when explicit control is needed.\n"
           "REMINDER\n");
#endif
#ifndef _WIN32
    fchmod(fileno(f), CLI_OCTAL_PERM);
#endif
    (void)fclose(f);
#ifdef _WIN32
    chmod(script_path, CLI_OCTAL_PERM);
#endif
}

int cbm_upsert_claude_session_hooks(const char *settings_path) {
    static const char *matchers[] = {"startup", "resume", "clear", "compact"};
    enum { MATCHER_COUNT = sizeof(matchers) / sizeof(matchers[0]) };
    char command[CLI_BUF_1K];
    if (!cbm_resolve_hook_command(CMM_SESSION_REMINDER_SCRIPT, command, sizeof(command))) {
        return CLI_ERR;
    }
    int rc = 0;
    for (int i = 0; i < MATCHER_COUNT; i++) {
        if (upsert_hooks_json((hooks_upsert_args_t){.settings_path = settings_path,
                                                    .hook_event = "SessionStart",
                                                    .matcher_str = matchers[i],
                                                    .command_str = command}) != 0) {
            rc = CLI_ERR;
        }
    }
    return rc;
}

int cbm_remove_claude_session_hooks(const char *settings_path) {
    static const char *matchers[] = {"startup", "resume", "clear", "compact"};
    enum { MATCHER_COUNT = sizeof(matchers) / sizeof(matchers[0]) };
    int rc = 0;
    for (int i = 0; i < MATCHER_COUNT; i++) {
        if (remove_hooks_json((hooks_remove_args_t){.settings_path = settings_path,
                                                    .hook_event = "SessionStart",
                                                    .matcher_str = matchers[i]}) != 0) {
            rc = CLI_ERR;
        }
    }
    return rc;
}

/* SubagentStart hook: subagents spawned via the Agent tool do NOT fire
 * SessionStart, so the SessionStart reminder above never reaches them. This
 * hook is their equivalent. Unlike SessionStart (where plain stdout is injected
 * as context), SubagentStart injects context only via a JSON object on stdout:
 *   {"hookSpecificOutput":{"hookEventName":"SubagentStart","additionalContext":"…"}}
 * The text is a leaner variant of the SessionStart protocol: it omits the
 * "run index_repository first" step, since the parent session has already
 * indexed the project. Matcher "*" fires for every agent type. */
#ifdef _WIN32
#define CMM_SUBAGENT_REMINDER_SCRIPT "cbm-subagent-reminder.cmd"
#else
#define CMM_SUBAGENT_REMINDER_SCRIPT "cbm-subagent-reminder"
#endif
#define CMM_SUBAGENT_REMINDER_SCRIPT_LEGACY "cbm-subagent-reminder"

static void cbm_install_subagent_reminder_script(const char *home) {
    if (!home) {
        return;
    }
    char config_dir[CLI_BUF_1K];
    cbm_claude_config_dir(home, config_dir, sizeof(config_dir));
    if (!config_dir[0]) {
        return;
    }
    char hooks_dir[CLI_BUF_1K];
    snprintf(hooks_dir, sizeof(hooks_dir), "%s/hooks", config_dir);
    cbm_mkdir_p(hooks_dir, CLI_OCTAL_PERM);

    cbm_remove_legacy_hook_script(hooks_dir, CMM_SUBAGENT_REMINDER_SCRIPT_LEGACY);
    char script_path[CLI_BUF_1K];
    snprintf(script_path, sizeof(script_path), "%s/" CMM_SUBAGENT_REMINDER_SCRIPT, hooks_dir);

    FILE *f = fopen(script_path, "w");
    if (!f) {
        return;
    }
    /* The additionalContext value is a single line with no embedded quotes,
     * backslashes, or newlines, so the JSON below is valid as written — no
     * runtime escaping (and no python3/jq dependency) is required. */
#ifdef _WIN32
    /* cmd variant: one echo with the JSON verbatim (quotes are literal in
     * cmd echo; no cmd metacharacters appear in the payload). */
    (void)fprintf(f, "@echo off\r\n"
                     "REM SubagentStart hook: tell subagents to use codebase-memory-mcp tools.\r\n"
                     "echo {\"hookSpecificOutput\":{\"hookEventName\":\"SubagentStart\","
                     "\"additionalContext\":\"Code discovery: prefer codebase-memory-mcp tools "
                     "(search_graph, trace_path, get_code_snippet, query_graph, get_architecture, "
                     "search_code) over grep/file-read for navigating code. Use Grep/Glob/Read for "
                     "text, configs, and non-code files.\"}}\r\n");
#else
    (void)fprintf(f,
                  "#!/usr/bin/env bash\n"
                  "# SubagentStart hook: tell subagents to use codebase-memory-mcp tools.\n"
                  "# Installed by codebase-memory-mcp. Fires when any subagent is spawned.\n"
                  "# SubagentStart injects context via JSON additionalContext, not plain stdout.\n"
                  "cat << 'REMINDER'\n"
                  "{\"hookSpecificOutput\":{\"hookEventName\":\"SubagentStart\","
                  "\"additionalContext\":\"Code discovery: prefer codebase-memory-mcp tools "
                  "(search_graph, trace_path, get_code_snippet, query_graph, get_architecture, "
                  "search_code) over grep/file-read for navigating code. Use Grep/Glob/Read for "
                  "text, configs, and non-code files.\"}}\n"
                  "REMINDER\n");
#endif
#ifndef _WIN32
    fchmod(fileno(f), CLI_OCTAL_PERM);
#endif
    (void)fclose(f);
#ifdef _WIN32
    chmod(script_path, CLI_OCTAL_PERM);
#endif
}

int cbm_upsert_claude_subagent_hooks(const char *settings_path) {
    char command[CLI_BUF_1K];
    cbm_resolve_hook_command(CMM_SUBAGENT_REMINDER_SCRIPT, command, sizeof(command));
    /* matcher "*" is the natural choice a user would also pick for their own
     * catch-all SubagentStart hook, so claim ownership by command too — never
     * clobber or remove a foreign "*" entry. */
    return upsert_hooks_json(
        (hooks_upsert_args_t){.settings_path = settings_path,
                              .hook_event = "SubagentStart",
                              .matcher_str = "*",
                              .command_str = command,
                              .match_command_substr = CMM_SUBAGENT_REMINDER_SCRIPT});
}

int cbm_remove_claude_subagent_hooks(const char *settings_path) {
    return remove_hooks_json(
        (hooks_remove_args_t){.settings_path = settings_path,
                              .hook_event = "SubagentStart",
                              .matcher_str = "*",
                              .match_command_substr = CMM_SUBAGENT_REMINDER_SCRIPT});
}

/* Matcher excludes read_file for consistency with the Claude fix: the hook
 * is an advisory reminder, not a gate over the agent's file reads. */
#define GEMINI_HOOK_MATCHER "google_search|grep_search"
#define GEMINI_HOOK_COMMAND                                               \
    "echo 'Reminder: prefer codebase-memory-mcp search_graph/trace_path/" \
    "get_code over grep/file search for code discovery.' >&2"

int cbm_upsert_gemini_hooks(const char *settings_path) {
    return upsert_hooks_json((hooks_upsert_args_t){
        .settings_path = settings_path,
        .hook_event = "BeforeTool",
        .matcher_str = GEMINI_HOOK_MATCHER,
        .command_str = GEMINI_HOOK_COMMAND,
        .old_matchers = cmm_gemini_old_matchers,
    });
}

int cbm_remove_gemini_hooks(const char *settings_path) {
    return remove_hooks_json((hooks_remove_args_t){
        .settings_path = settings_path,
        .hook_event = "BeforeTool",
        .matcher_str = GEMINI_HOOK_MATCHER,
        .old_matchers = cmm_gemini_old_matchers,
    });
}

/* Gemini CLI / Antigravity SessionStart reminder. settings.json uses the same
 * hooks.<Event>[].hooks[] JSON shape as Claude, so it reuses upsert_hooks_json.
 * The SessionStart matcher is advisory in Gemini (it does not filter lifecycle
 * sources), so a single "startup" entry fires on startup/resume/clear. The
 * command's stdout is injected as session context. (Gemini/Antigravity parity
 * with the Claude/Codex SessionStart reminder.) */
int cbm_upsert_gemini_session_hooks(const char *settings_path) {
    return upsert_hooks_json((hooks_upsert_args_t){
        .settings_path = settings_path,
        .hook_event = "SessionStart",
        .matcher_str = "startup",
        .command_str = CMM_SESSION_REMINDER_CMD,
    });
}

int cbm_remove_gemini_session_hooks(const char *settings_path) {
    return remove_hooks_json((hooks_remove_args_t){
        .settings_path = settings_path,
        .hook_event = "SessionStart",
        .matcher_str = "startup",
    });
}

/* ── PATH management ──────────────────────────────────────────── */

int cbm_ensure_path(const char *bin_dir, const char *rc_file, bool dry_run) {
    if (!bin_dir || !rc_file) {
        return CLI_ERR;
    }

    /* fish uses a different syntax than POSIX shells: `export PATH="...:$PATH"`
     * is a syntax error in fish and breaks config.fish (#319). When the target
     * is a fish config, emit the fish-native `fish_add_path` (idempotent,
     * prepends only if absent) instead. */
    size_t rc_len = strlen(rc_file);
    bool is_fish = rc_len >= CBM_SZ_5 && strcmp(rc_file + rc_len - CBM_SZ_5, ".fish") == 0;

    char line[CLI_BUF_1K];
    if (is_fish) {
        snprintf(line, sizeof(line), "fish_add_path %s", bin_dir);
    } else {
        snprintf(line, sizeof(line), "export PATH=\"%s:$PATH\"", bin_dir);
    }

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
static const size_t ZIP_MAX_UNCOMP = 500U * 1024U * 1024U;

static uint16_t zip_read_u16le(const unsigned char *p) {
    return (uint16_t)((uint16_t)p[0] | ((uint16_t)p[1] << BYTE_SHIFT));
}

static uint32_t zip_read_u32le(const unsigned char *p) {
    return ((uint32_t)p[0]) | ((uint32_t)p[1] << BYTE_SHIFT) |
           ((uint32_t)p[2] << (BYTE_SHIFT * CLI_PAIR_LEN)) |
           ((uint32_t)p[3] << (BYTE_SHIFT * CLI_JSON_INDENT));
}

/* Decompress a single zip entry (stored or deflated). Returns malloc'd buffer
 * or NULL on failure. *out_len receives the decompressed size. */
static unsigned char *zip_extract_entry(const unsigned char *file_data, uint16_t method,
                                        size_t comp_size, size_t uncomp_size, int *out_len) {
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
        if (comp_size > UINT_MAX || uncomp_size > UINT_MAX) {
            return NULL;
        }
        unsigned char *out = malloc(uncomp_size);
        if (!out) {
            return NULL;
        }
        z_stream strm = {0};
        strm.next_in = (unsigned char *)file_data;
        strm.avail_in = (uInt)comp_size;
        strm.next_out = out;
        strm.avail_out = (uInt)uncomp_size;
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

        uint16_t method = zip_read_u16le(data + pos + ZIP_OFF_METHOD);
        uint32_t comp_size = zip_read_u32le(data + pos + ZIP_OFF_COMP);
        uint32_t uncomp_size = zip_read_u32le(data + pos + ZIP_OFF_UNCOMP);
        uint16_t name_len = zip_read_u16le(data + pos + ZIP_OFF_NAMELEN);
        uint16_t extra_len = zip_read_u16le(data + pos + ZIP_OFF_EXTRALEN);

        int header_end = pos + ZIP_HDR_SZ + name_len + extra_len;
        if (header_end > data_len || comp_size > (uint32_t)(data_len - header_end)) {
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

static bool is_project_index_db_name(const char *name) {
    if (!name) {
        return false;
    }
    size_t len = strlen(name);
    if (len <= DB_EXT_LEN || strcmp(name + len - DB_EXT_LEN, ".db") != 0) {
        return false;
    }
    return strcmp(name, "_config.db") != 0;
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
        if (is_project_index_db_name(ent->name)) {
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
        if (is_project_index_db_name(ent->name)) {
            char path[CLI_BUF_1K];
            int path_len = snprintf(path, sizeof(path), "%s/%s", cache_dir, ent->name);
            if (path_len < 0 || (size_t)path_len >= sizeof(path)) {
                (void)fprintf(stderr,
                              "warning: skipping index cleanup entry with overlong path: %s/%s\n",
                              cache_dir, ent->name);
                continue;
            }
            /* Also remove .db.tmp if present */
            char tmp_path[CLI_FIELD_1040];
            int tmp_len = snprintf(tmp_path, sizeof(tmp_path), "%s.tmp", path);
            if (tmp_len >= 0 && (size_t)tmp_len < sizeof(tmp_path)) {
                cbm_unlink(tmp_path);
            } else {
                (void)fprintf(stderr,
                              "warning: skipping overlong temporary sidecar cleanup for %s\n",
                              path);
            }
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

double cbm_config_get_double(cbm_config_t *cfg, const char *key, double default_val) {
    const char *val = cbm_config_get(cfg, key, NULL);
    if (!val) {
        return default_val;
    }
    char *endptr;
    double v = strtod(val, &endptr);
    if (endptr == val || *endptr != '\0') {
        return default_val;
    }
    return v;
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

/* ── Config registry ──────────────────────────────────────────── */

/* Hand-wrapped for readable help text; automatic formatting makes this table
 * substantially narrower and churns unrelated entries. */
// clang-format off
const cbm_config_entry_t CBM_CONFIG_REGISTRY[] = {
    /* ── Indexing ── */
    {"auto_index", "true", "CBM_AUTO_INDEX", "Indexing",
     "Auto-index the MCP server CWD or explicit repo paths on startup/first use",
     "true|false",
     "Enable for automatic indexing; disable for manual control, CI, or embedded read-only contexts."},
    {"auto_index_limit", "50000", "CBM_AUTO_INDEX_LIMIT", "Indexing",
     "Max indexable files before auto-index is skipped (0=no limit, index everything)",
     "0-10000000",
     "Protects against accidentally indexing huge monorepos. Raise for large codebases. "
     "Set 0 to disable the limit and always index regardless of repo size."},
    {CBM_CONFIG_EXTRACT_TIMEOUT_MS, CBM_CONFIG_EXTRACT_TIMEOUT_DEFAULT, NULL, "Indexing",
     "Per-file tree-sitter parse deadline in milliseconds",
     "100-120000",
     "Bounds pathological parses. A timeout fails the indexing transaction and preserves the prior database; raise only for measured large-file workloads or instrumented tests."},
    {"reindex_on_startup", "false", "CBM_REINDEX_ON_STARTUP", "Indexing",
     "Re-index stale projects when server starts",
     "true|false",
     "Enable to refresh stale indexes at startup (adds startup latency). Prefer reindex_stale_seconds for scheduled refresh."},
    {"reindex_stale_seconds", "0", NULL, "Indexing",
     "Re-index if DB is older than N seconds (0=disabled)",
     "0-2592000",
     "0=disabled. 3600=hourly, 86400=daily, 604800=weekly. Runs on startup if stale."},
    {CBM_CONFIG_INCREMENTAL_REINDEX, CBM_CONFIG_INCREMENTAL_REINDEX_OFF, NULL, "Indexing",
     "When to use the disk incremental reindex path",
     "fast|always|off",
     "'off' rebuilds atomically from scratch and is the default until disk incremental avoids full-graph "
     "work. 'fast' uses incremental only for fast-mode indexes. 'always' preserves the legacy route for "
     "benchmarking and canary tests."},
    {CBM_CONFIG_OVERLAY_PUBLISH, CBM_CONFIG_OVERLAY_PUBLISH_OFF, NULL, "Indexing",
     "Foreground overlay publish policy for bounded incremental deltas",
     "off|small_deltas",
     "'off' preserves canonical publish behavior. 'small_deltas' is opt-in: eligible exact-delta "
     "batches publish ready overlay rows without mutating canonical graph rows; a canonical full "
     "reindex remains the repair/oracle path."},
    {CBM_CONFIG_OVERLAY_COMPACTION_POLICY,
     CBM_CONFIG_OVERLAY_COMPACTION_POLICY_MANUAL,
     NULL,
     "Indexing",
     "Background overlay compaction trigger policy",
     "manual|after_publish",
     "'manual' never starts compaction automatically. 'after_publish' starts one bounded worker "
     "only after index_repository successfully publishes an incremental_overlay result."},
    {CBM_CONFIG_OVERLAY_COMPACTION_MAX_GENERATIONS,
     CBM_CONFIG_OVERLAY_COMPACTION_DEFAULT_MAX_GENERATIONS,
     NULL,
     "Indexing",
     "Max overlay generations compacted by one automatic worker pass",
     "1-256",
     "Bounds after_publish maintenance. Keep low for foreground responsiveness; raise only after "
     "benchmarks show compaction backlog is the limiting factor."},
    {CBM_CONFIG_INCREMENTAL_EXACT_MAX_CHANGED_PATHS,
     CBM_CONFIG_INCREMENTAL_EXACT_DEFAULT_MAX_CHANGED_PATHS,
     NULL,
     "Indexing",
     "Max changed files eligible for exact disk incremental publish",
     "1-100000",
     "Default is conservative. Raise only with canonical-graph benchmarks for your workload; larger "
     "batches can approach full-rebuild cost."},
    {CBM_CONFIG_INCREMENTAL_EXACT_MAX_AFFECTED_PATHS,
     CBM_CONFIG_INCREMENTAL_EXACT_DEFAULT_MAX_AFFECTED_PATHS,
     NULL,
     "Indexing",
     "Max changed plus inbound-dependent source files exact delta may reparse",
     "1-100000",
     "Default 16 keeps small cross-file C/C++ edit frontiers on the canonical exact path. The cap "
     "limits exact-delta work before a correctness fallback; it does not bound total indexing cost "
     "because fallback may perform containment or a full rebuild. Change only with canonical-graph "
     "and latency/memory benchmarks for your workload."},
    {CBM_CONFIG_INCREMENTAL_DERIVED_REFRESH,
     CBM_CONFIG_INCREMENTAL_DERIVED_REFRESH_DEFAULT,
     NULL,
     "Indexing",
     "When incremental publishes may defer global semantic/similarity edge refresh",
     CBM_CONFIG_INCREMENTAL_DERIVED_REFRESH_EAGER "|"
     CBM_CONFIG_INCREMENTAL_DERIVED_REFRESH_STALE_ON_EXACT "|"
     CBM_CONFIG_INCREMENTAL_DERIVED_REFRESH_STALE_ON_INCREMENTAL,
     "'stale_on_incremental' (default): incremental publishes mark semantic_edges stale and defer "
     "global semantic/similarity refresh; query surfaces warn until a full/eager run rebuilds them. "
     "'eager' preserves full/moderate publish freshness synchronously. 'stale_on_exact' lets small exact "
     "incremental graph deltas publish after marking semantic_edges stale; semantic/similarity "
     "queries warn until an eager index run or full reindex rebuilds those global edges."},
    /* ── Search ── */
    {"search_limit", "50", NULL, "Search",
     "Default max results for search_graph/search_code",
     "1-100000",
     "Higher = more results but more tokens. Overridden by limit param per-query. "
     "50 is good for exploration; 200+ for exhaustive analysis."},
    {"trace_max_results", "25", NULL, "Search",
     "Default max nodes per direction in trace_path",
     "1-10000",
     "Controls how far call chains are traced. 25 covers typical call depth; raise to 100+ for deep dependency tracing."},
    {CBM_CONFIG_QUERY_MAX_ROWS, CBM_DEFAULT_QUERY_MAX_ROWS_STR, NULL, "Search",
     "Default scan-level row cap for query_graph when max_rows is omitted",
     "0-1000000",
     "Matches upstream's 100000-row Cypher ceiling by default. Lower to reduce latency and memory for broad queries; use per-call max_rows for one query."},
    {"query_max_output_bytes", "32768", NULL, "Search",
     "Max response bytes for query_graph (0=unlimited)",
     "0-104857600",
     "32KB default prevents huge responses. Set 0 for unlimited Cypher results. Raise for bulk analysis queries."},
    {"snippet_max_lines", "200", NULL, "Search",
     "Max source lines returned by get_code/get_code_snippet (0=unlimited)",
     "0-1000000",
     "200 lines covers most functions. Set 0 for unlimited to get full file contents."},
    {"key_functions_exclude", "", "CBM_KEY_FUNCTIONS_EXCLUDE", "Search",
     "Comma-separated glob patterns to exclude from architecture key functions",
     "glob patterns, e.g. graph-ui/**,tests/**",
     "Use to remove UI, generated code, or test helpers from the architecture view. "
     "Example: 'graph-ui/**,tools/**,scripts/**,tests/**'."},
    {"key_functions_count", "25", NULL, "Search",
     "Max key functions returned in codebase://architecture and search context",
     "1-10000",
     "The architecture resource ranks every symbol by PageRank importance and returns the top N. "
     "Use 25 for most projects. Raise to 50-100 for large multi-language codebases where "
     "important functions may not appear in the first 25. Lower to 10 when tokens are limited."},
    {"context_key_functions_limit", "10", NULL, "Search",
     "Max key functions PUSHED in the first-response _context header (0 = use built-in 10)",
     "0-100",
     "Separate from key_functions_count (the architecture query bound) — this governs only the "
     "auto-pushed summary that closes the codebase://architecture pull-only gap. Kept small (10) "
     "to keep first-response token cost modest; raise to 20-25 if you want richer upfront context."},
    /* ── Tools ── */
    {"tool_mode", "streamlined", "CBM_TOOL_MODE", "Tools",
     "Which tool surface the MCP server lists by default",
     "streamlined|classic",
     "'streamlined' (default): lists core tools plus _hidden_tools discovery: "
     "search_graph, query_graph, search_code, trace_path, get_code. "
     "'classic': exposes all individual tools including index_repository, get_code_snippet, get_architecture, "
     "list_projects, detect_changes, manage_adr, etc. "
     "You can also enable individual classic tools without switching modes: "
     "config set tool_index_repository true"},
    {"context_injection", "true", "CBM_CONTEXT_INJECTION", "Tools",
     "Inject codebase schema and stats into the first tool response so the AI starts informed",
     "true|false",
     "When true (default), the first search_graph response includes a "
     "_context object: node/edge counts, node labels, edge types, PageRank status, and "
     "detected language ecosystem. Delivered once per MCP server process; later calls are unaffected. "
     "Why enable: the MCP client gets codebase structure upfront without needing to call "
     "get_architecture or get_graph_schema separately. Useful for code exploration, "
     "refactoring, debugging, and codebase-understanding tasks. "
     "Why disable: context window space consumed by _context is wasted when the MCP client task "
     "involves non-code tasks, scripted/programmatic tool use, CI pipelines, token-metered "
     "environments, or when the model already has codebase context from another source. "
     "To disable for one MCP server process: export CBM_CONTEXT_INJECTION=false "
     "To disable by default: codebase-memory-mcp config set context_injection false"},
    {"compact", "true", "CBM_COMPACT", "Tools",
     "Default compact output for search_graph, trace_path, and get_code",
     "true|false",
     "true (default): omits name when equal to last qn segment, empty label/file, degree=0. "
     "Per-call compact= param overrides. false for programmatic output parsing."},
    {"default_sort_by", "relevance", NULL, "Tools",
     "Default sort for search_graph when sort_by not specified",
     "relevance|name|degree|calls|linkrank",
     "relevance = PageRank structural importance. calls = most direct calls. "
     "Set 'calls' for call-density analysis workflows."},
    {"default_include_dependencies", "true", NULL, "Tools",
     "Default include_dependencies for search_graph",
     "true|false",
     "false = restrict to project code only (exclude dep sub-projects). "
     "Set false for single-project focus workflows."},
    {"search_disable_dep_ranking", "false", NULL, "Tools",
     "Disable project-before-dependency ranking in search_graph",
     "true|false",
     "false (default) ranks project symbols before dependency symbols when include_dependencies=true. "
     "true uses pure relevance order across project and dependency symbols."},
    /* ── PageRank ── */
    {CBM_CONFIG_RANK_ENABLED, "true", NULL, "PageRank",
     "Compute PageRank, LinkRank, and precomputed node-degree views after indexing",
     "true|false",
     "true preserves existing ranking behavior. false skips all three coupled rank views and removes "
     "their stored rows so queries cannot consume stale scores; structural degree remains available. "
     "Disable for a lower-cost baseline: codebase-memory-mcp config set rank_enabled false"},
    {"pagerank_max_iter", "20", NULL, "PageRank",
     "Max iterations for PageRank algorithm before stopping (more = more accurate convergence)",
     "1-10000",
     "PageRank is an iterative algorithm — each iteration refines importance scores. "
     "20 iterations converges in ~5ms for 16K-node codebases. Typical convergence is 10-15 iters. "
     "Raise to 50-100 for very large codebases (>100K nodes). "
     "Diminishing returns above convergence — set too high wastes CPU at reindex time."},
    {"pagerank_damping", "0.85", NULL, "PageRank",
     "Damping factor — fraction of importance that follows edges vs. teleports randomly (the 'bored surfer')",
     "0.0-1.0",
     "0.85 is the standard Google PageRank value. Higher (0.9) spreads importance further along long "
     "call chains; lower (0.7-0.8) keeps importance local to direct callers. Out-of-range and NaN "
     "values are clamped to 0.85 at compute time, so an invalid value never crashes indexing."},
    {"pagerank_epsilon", "0.000001", NULL, "PageRank",
     "Convergence threshold — PageRank stops early when the L2 change between iterations drops below this",
     "0.0-1.0",
     "1e-6 default. Lower (1e-8) iterates longer for marginally finer convergence (rarely needed); "
     "higher (1e-4) stops sooner with slightly less precise rankings. Must be > 0 — non-positive and "
     "NaN values are clamped to 1e-6. Rarely needs tuning; pair with pagerank_max_iter as the hard cap."},
    {"rank_scope", "full", NULL, "PageRank",
     "Project/dependency scope used when computing PageRank and LinkRank",
     "full|project|deps",
     "'full' (default): score the project plus its dependency sub-projects. "
     "'project': score only the requested project's own symbols. "
     "'deps': score only dependency sub-project symbols."},
    {"rank_refresh", CBM_RANK_REFRESH_DEFAULT, NULL, "PageRank",
     "When to recompute PageRank/LinkRank after indexing",
     CBM_RANK_REFRESH_EAGER "|" CBM_RANK_REFRESH_STALE_ON_EXACT "|"
     CBM_RANK_REFRESH_STALE_ON_INCREMENTAL,
     "'stale_on_incremental' (default): incremental publishes, including safe full fallbacks, may "
     "defer rank recompute after marking rank views stale; search/trace omit stale rank until a "
     "later refresh. "
     "'eager': recompute after graph changes, dependency reindexes, or missing rank views. "
     "'stale_on_exact': exact incremental graph deltas may skip synchronous rank recompute only after "
     "rank views are marked stale. 'stale_on_incremental': also allows containment publishes and full "
     "rebuilds reached through incremental fallback to defer; search/trace then omit stale rank until "
     "a refresh runs."},
    {"edge_weight_calls", "1.0", NULL, "PageRank",
     "How much importance flows along direct function/method call edges (CALLS)",
     "0.0-100.0",
     "PageRank works like Google PageRank: importance flows along edges. Higher weight = more "
     "importance flows when one function calls another. 1.0 is the anchor — all other weights "
     "are relative to it. Increase to 2.0 for call-heavy C/Rust codebases. "
     "Decrease to 0.5 for event-driven systems where direct calls aren't the primary coupling."},
    {"edge_weight_usage", "0.7", NULL, "PageRank",
     "How much importance flows along type-reference edges: type annotations, attribute access, isinstance (USAGE)",
     "0.0-100.0",
     "USAGE edges are created when code references a type (e.g. 'x: MyClass', 'isinstance(x, Foo)'). "
     "These are dense in TypeScript/Python and can inflate UI utilities over core functions. "
     "Reduce to 0.2-0.3 if type annotations are dominating your architecture results."},
    {"edge_weight_defines_method", "0.5", NULL, "PageRank",
     "How much importance flows from a class to each method it defines (DEFINES_METHOD)",
     "0.0-100.0",
     "Every class has one DEFINES_METHOD edge per method. Higher = classes with many methods rank "
     "higher relative to standalone functions. Lower to 0.1 to treat functions and class methods equally."},
    {"edge_weight_imports", "0.3", NULL, "PageRank",
     "How much importance flows along module import edges (IMPORTS)",
     "0.0-100.0",
     "Created when file A imports file/module B. Higher promotes widely-imported utility modules "
     "(e.g. a shared 'utils.py' imported by 50 files). Raise to 0.6-0.8 to emphasize shared infrastructure; "
     "keep low if star-imports create many spurious edges."},
    {"edge_weight_decorates", "0.2", NULL, "PageRank",
     "How much importance flows from a decorator to the function it decorates (DECORATES)",
     "0.0-100.0",
     "Created when @decorator is applied to a function. Raise to 0.5+ in Python web frameworks "
     "where @route, @cached, @requires_auth are semantically important architectural markers."},
    {"edge_weight_writes", "0.15", NULL, "PageRank",
     "How much importance flows when a function writes to a variable or file (WRITES)",
     "0.0-100.0",
     "Tracks side effects: function writes to a shared variable or file. Raise for ETL or "
     "data-pipeline codebases where write targets (databases, output files) are the primary output."},
    {"edge_weight_defines", "0.1", NULL, "PageRank",
     "How much importance flows from a file/module to each symbol it defines (DEFINES — structural)",
     "0.0-100.0",
     "Every function has exactly one DEFINES edge from its containing file. This is purely structural "
     "bookkeeping — keep very low (0.01-0.1). Raising this inflates ALL symbols in a file equally, "
     "which is rarely what you want."},
    {"edge_weight_configures", "0.1", NULL, "PageRank",
     "How much importance flows from config files to the code they configure (CONFIGURES)",
     "0.0-100.0",
     "Created when a config file references a code symbol (e.g. a YAML file referencing a handler "
     "class). Raise to 0.3+ for infrastructure projects where config -> code coupling is important."},
    {"edge_weight_tests", "0.05", NULL, "PageRank",
     "How much importance flows from test code to the production function it tests (TESTS)",
     "0.0-100.0",
     "Intentionally very low so test files don't inflate production function rankings. A function "
     "with 100 tests would otherwise rank at the top of every project. Raise only if you want "
     "heavily-tested functions to rank higher (useful for spotting critical code paths)."},
    {"edge_weight_http_calls", "0.5", NULL, "PageRank",
     "How much importance flows along cross-service HTTP call edges (HTTP_CALLS)",
     "0.0-100.0",
     "Created when code makes an HTTP call to another service endpoint. Raise to 1.0-2.0 for "
     "microservice architectures where HTTP calls ARE the primary coupling between components "
     "and you want service entry points to appear prominently in architecture results."},
    {"edge_weight_async_calls", "0.8", NULL, "PageRank",
     "How much importance flows along async function call edges (ASYNC_CALLS)",
     "0.0-100.0",
     "Like edge_weight_calls but for async/await call patterns. Slightly lower than sync calls "
     "by default. Reduce to 0.3 for heavily async Node.js or Python asyncio codebases where "
     "awaited spans are dense and create noise in the rankings."},
    {"edge_weight_default", "0.1", NULL, "PageRank",
     "Fallback importance weight for edge types not listed above",
     "0.0-100.0",
     "Safety net for any edge types added in future without explicit weights. "
     "Rarely affects results. Keep low."},
    {"edge_weight_member_of", "0.5", NULL, "PageRank",
     "How much importance flows from a method back up to its parent class (MEMBER_OF — reverse structural)",
     "0.0-100.0",
     "Set to 0 to disable (method importance stays in the method, not the class). "
     "Higher values propagate method-level importance up to the parent class — "
     "raise to 0.8 to make heavily-called classes rank higher than individual methods."},
    /* ── Watcher ── */
    {CBM_CONFIG_AUTO_WATCH, "true", NULL, "Watcher",
     "Register the background Git watcher when an MCP session connects",
     "true|false",
     "Disable for hermetic or externally orchestrated indexing; enable for automatic incremental refresh."},
    {"watcher_poll_base_ms", "5000", NULL, "Watcher",
     "Base file-watcher poll interval in milliseconds",
     "100-3600000",
     "5 seconds by default. Lower for faster change detection (100ms for dev loops); "
     "raise for large repos to reduce CPU overhead. Actual interval scales with file count."},
    {"watcher_poll_max_ms", "60000", NULL, "Watcher",
     "Maximum file-watcher poll interval in milliseconds (cap for large repos)",
     "100-3600000",
     "60 seconds for repos with 50K+ files. Lower to 10000 for faster detection in large repos "
     "if CPU allows. Formula: min(base + file_count/500 * 1000, max)."},
    {CBM_CONFIG_UI_LANG, "auto", NULL, "UI",
     "Graph UI language selection",
     "auto|en|zh",
     "Use auto to follow the client locale, or pin a supported language for consistent shared sessions."},
    {"store_idle_timeout_s", "60", NULL, "MCP",
     "Seconds an MCP server keeps an idle SQLite project store open",
     "1-65536",
     "60 seconds balances latency for repeated tool calls with memory release while idle. Lower for "
     "memory-constrained hosts; raise for long MCP server runs that repeatedly query one project."},
    {"db_validate_busy_timeout_ms", "1000", NULL, "MCP",
     "SQLite busy timeout for read-only cache database validation in MCP startup/discovery paths",
     "0-65536",
     "1 second avoids hanging JSON-RPC startup on locked databases. Raise on slow network filesystems; "
     "set 0 to fail immediately."},
    {"update_check_timeout_s", "5", NULL, "MCP",
     "Curl timeout for the optional MCP background latest-release check (0=disabled)",
     "0-256",
     "Default preserves the historical 5-second bound. Set 0 for offline, hermetic, or privacy-sensitive "
     "MCP deployments where the server must not make background network requests."},
    /* ── Architecture ── */
    {"arch_hotspot_limit", "25", NULL, "Architecture",
     "Max hotspot functions shown in the classic get_architecture tool's hotspots section",
     "1-10000",
     "Hotspots are functions ranked by how many times they are directly called (calls_in count). "
     "They identify the most-invoked code — good candidates for optimization and risk assessment. "
     "25 is enough for orientation; raise to 100 for exhaustive call-density analysis. "
     "Only applies to the classic 'get_architecture' tool (tool_mode=classic)."},
    {"architecture_resolution", "1.0", NULL, "Architecture",
     "Leiden community-detection resolution for architecture clusters",
     "0.0001-10.0",
     "1.0 is the standard Leiden default. Higher (2.0-5.0) splits code into more, finer-grained "
     "clusters; lower (0.3-0.5) merges related clusters into coarse subsystems. Non-positive and "
     "NaN values are clamped to 1.0. Drives the 'clusters' section of get_architecture."},
    /* ── Similarity ── */
    {CBM_CONFIG_SIMILARITY_ENABLED, "true", NULL, "Similarity",
     "Create MinHash SIMILAR edges during full and moderate indexing",
     "true|false",
     "false skips the global MinHash comparison pass while leaving semantic edges independent. "
     "Use for an ablation or lower indexing cost: codebase-memory-mcp config set similarity_enabled false"},
    {CBM_CONFIG_SEMANTIC_EDGES_ENABLED, "true", NULL, "Similarity",
     "Create SEMANTICALLY_RELATED edges during full and moderate indexing",
     "true|false",
     "false skips the global semantic-edge pass while leaving MinHash similarity independent. "
     "Use for an ablation or lower indexing cost: codebase-memory-mcp config set semantic_edges_enabled false"},
    {CBM_CONFIG_GITHISTORY_ENABLED, "true", NULL, "Similarity",
     "Scan Git history and create FILE_CHANGES_WITH coupling edges",
     "true|false",
     "false avoids Git-history scanning and its worker without changing source extraction. "
     "Disable for repositories without useful history: codebase-memory-mcp config set githistory_enabled false"},
    {CBM_CONFIG_HTTPLINKS_ENABLED, "true", NULL, "Similarity",
     "Create route-to-client HTTP_CALLS edges with the HTTP linker",
     "true|false",
     "false skips fork-specific HTTP linking while retaining route discovery and other call edges. "
     "Use for upstream-compatible comparisons: codebase-memory-mcp config set httplinks_enabled false"},
    {"similarity_threshold", "0.0", NULL, "Similarity",
     "MinHash Jaccard threshold for semantic SIMILAR edges (0.0 = use the built-in 0.95 default)",
     "0.0-1.0",
     "Two symbols get a SIMILAR edge when their estimated Jaccard similarity is at least this. "
     "0.0 (default) uses the built-in 0.95, so only near-duplicates are linked. Lower to 0.7-0.8 to "
     "surface more semantic duplicates (refactoring candidates); too low adds noisy edges that inflate "
     "PageRank rankings. Effective only when indexing creates similarity edges (full/moderate modes)."},
    {"semantic_threshold", "0.0", NULL, "Similarity",
     "Combined semantic score threshold for SEMANTICALLY_RELATED edges (0.0 = built-in 0.75)",
     "0.0-1.0",
     "Controls the algorithmic semantic-edge pass. Higher values improve precision and reduce edge count; "
     "lower values increase recall and runtime output volume. 0.0 preserves the upstream-compatible default."},
    {"httplink_min_confidence", "0.0", NULL, "Similarity",
     "Minimum confidence for HTTP route-to-call linking (0.0 = built-in 0.25)",
     "0.0-1.0",
     "Raises or lowers the fork-only HTTP linker's match threshold. Higher values reduce speculative "
     "cross-service HTTP_CALLS edges; 0.0 keeps existing behavior."},
    {"githistory_min_coupling", "0.0", NULL, "Similarity",
     "Minimum file co-change coupling score for FILE_CHANGES_WITH edges (0.0 = built-in 0.3)",
     "0.0-1.0",
     "Controls how strongly two files must co-change before git-history coupling emits an edge. "
     "Higher values reduce noisy historical edges; 0.0 keeps existing behavior."},
    {"lsp_confidence_floor", "0.0", NULL, "Similarity",
     "Minimum LSP-resolved call confidence accepted by call resolution (0.0 = built-in 0.6)",
     "0.0-1.0",
     "Applies consistently to sequential and parallel call resolution. Raise to prefer registry matches "
     "over uncertain LSP hints; lower only when language-specific LSP coverage is known to be precise."},
    /* ── Degree / Sort ── */
    {"degree_mode", "weighted", NULL, "Degree",
     "What 'degree' means for min_degree/max_degree filters and sort_by=degree ranking",
     "weighted|unweighted|calls_only",
     "Degree = how connected a symbol is. 'weighted' multiplies each connection by its edge type weight "
     "(e.g. a direct call counts 1.0x, a test call counts 0.05x) — best overall signal. "
     "'unweighted' = raw connection count regardless of type. "
     "'calls_only' = only count direct function call connections — best for finding the most-called functions."},
    /* ── Dependencies ── */
    {"auto_index_deps", "true", NULL, "Dependencies",
     "Auto-index installed packages from package.json, Cargo.toml, go.mod, etc.",
     "true|false",
     "Enable to trace calls into dependencies (e.g. find all callers of a library function). "
     "Disable for faster indexing when cross-package search is not needed."},
    {"auto_dep_limit", "20", NULL, "Dependencies",
     "Max number of packages to auto-index",
     "0-10000",
     "20 covers the most-used imports. Raise to 100+ for comprehensive dependency analysis. "
     "0 = unlimited (may be very slow for large dependency trees)."},
    {"dep_max_files", "1000", NULL, "Dependencies",
     "Max source files per dependency package (0=unlimited)",
     "0-1000000",
     "Caps indexing of large packages (TensorFlow, LLVM). 1000 covers most packages. "
     "Set 0 for unlimited if you need complete large-package analysis."},
    {NULL, NULL, NULL, NULL, NULL, NULL, NULL} /* sentinel */
};
// clang-format on

/* Get config value with env var override priority: env > db > default.
 * Looks up the registry entry for the key to find the env var name. */
const char *cbm_config_get_effective(cbm_config_t *cfg, const char *key, const char *default_val) {
    /* Check env var override first */
    for (int i = 0; CBM_CONFIG_REGISTRY[i].key; i++) {
        if (strcmp(CBM_CONFIG_REGISTRY[i].key, key) == 0 && CBM_CONFIG_REGISTRY[i].env_var) {
            // NOLINTNEXTLINE(concurrency-mt-unsafe)
            const char *env = getenv(CBM_CONFIG_REGISTRY[i].env_var);
            if (env && env[0]) return env;
            break;
        }
    }
    /* Fall back to DB value or default */
    return cbm_config_get(cfg, key, default_val);
}

bool cbm_config_get_effective_bool(cbm_config_t *cfg, const char *key, bool default_val) {
    const char *val = cbm_config_get_effective(cfg, key, default_val ? "true" : "false");
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

int cbm_config_get_effective_int(cbm_config_t *cfg, const char *key, int default_val) {
    char default_buf[CBM_SZ_32];
    snprintf(default_buf, sizeof(default_buf), "%d", default_val);
    const char *val = cbm_config_get_effective(cfg, key, default_buf);
    if (!val || !val[0]) {
        return default_val;
    }
    char *endptr;
    long parsed = strtol(val, &endptr, CLI_STRTOL_BASE);
    if (endptr == val || *endptr != '\0') {
        return default_val;
    }
    return (int)parsed;
}

/* ── Config CLI subcommand ────────────────────────────────────── */

int cbm_cmd_config(int argc, char **argv) {
    if (argc == 0) {
        printf("Usage: codebase-memory-mcp config <command> [args]\n\n");
        printf("Commands:\n");
        printf("  list             Show all config values (with env overrides)\n");
        printf("  get <key>        Get effective value (env > db > default)\n");
        printf("  set <key> <val>  Set a config value\n");
        printf("  reset <key>      Reset a key to default\n\n");
        printf("Storage: ~/.cache/codebase-memory-mcp/_config.db\n");
        printf("Priority: environment variable > config set > default\n\n");
        printf("Examples:\n");
        printf("  codebase-memory-mcp config set auto_index false\n");
        printf("  codebase-memory-mcp config set key_functions_exclude \"scripts/**,tests/**\"\n");
        printf("  CBM_AUTO_INDEX=false codebase-memory-mcp   # env override for one run\n");
        printf("  export CBM_TOOL_MODE=classic               # env override for session\n\n");
        /* Print keys grouped by category with env var info */
        printf("Config keys:\n");
        const char *last_cat = "";
        for (int i = 0; CBM_CONFIG_REGISTRY[i].key; i++) {
            const cbm_config_entry_t *e = &CBM_CONFIG_REGISTRY[i];
            if (strcmp(e->category, last_cat) != 0) {
                if (i > 0) printf("\n");
                printf("  [%s]\n", e->category);
                last_cat = e->category;
            }
            if (e->env_var) {
                printf("  %-30s default=%-14s [env: %s]\n",
                    e->key, e->default_val, e->env_var);
            } else {
                printf("  %-30s default=%-14s\n",
                    e->key, e->default_val);
            }
            if (e->range || e->description)
                printf("      [%-20s]  %s\n",
                       e->range ? e->range : "any",
                       e->description ? e->description : "");
            if (e->guidance)
                printf("      %s\n\n", e->guidance);
        }
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
        const char *last_cat = "";
        for (int i = 0; CBM_CONFIG_REGISTRY[i].key; i++) {
            const cbm_config_entry_t *e = &CBM_CONFIG_REGISTRY[i];
            /* Print category header when it changes */
            if (strcmp(e->category, last_cat) != 0) {
                if (i > 0) printf("\n");
                printf("[%s]\n", e->category);
                last_cat = e->category;
            }
            const char *val = cbm_config_get_effective(cfg, e->key, e->default_val);
            /* Check if env var is active */
            const char *source = "";
            if (e->env_var) {
                // NOLINTNEXTLINE(concurrency-mt-unsafe)
                const char *env = getenv(e->env_var);
                if (env && env[0]) source = " (env)";
            }
            /* Check if DB value differs from default */
            const char *db_val = cbm_config_get(cfg, e->key, NULL);
            if (!source[0] && db_val) source = " (set)";
            printf("  %-30s = %-14s%s\n", e->key, val, source);
            if (e->range || e->description)
                printf("      [%-20s]  %s\n",
                       e->range ? e->range : "any",
                       e->description ? e->description : "");
            if (e->guidance)
                printf("      %s\n\n", e->guidance);
        }
    } else if (strcmp(argv[0], "get") == 0) {
        if (argc < MIN_ARGC_GET) {
            (void)fprintf(stderr, "Usage: config get <key>\n");
            rc = CLI_TRUE;
        } else {
            /* Find default from registry (fork: richer `config get` that shows
             * effective value plus range/guidance; superset of upstream's
             * single-line cbm_config_get() print). */
            const char *def = "";
            const cbm_config_entry_t *found_entry = NULL;
            for (int i = 0; CBM_CONFIG_REGISTRY[i].key; i++) {
                if (strcmp(CBM_CONFIG_REGISTRY[i].key, argv[1]) == 0) {
                    def = CBM_CONFIG_REGISTRY[i].default_val;
                    found_entry = &CBM_CONFIG_REGISTRY[i];
                    break;
                }
            }
            printf("%s\n", cbm_config_get_effective(cfg, argv[1], def));
            if (found_entry) {
                if (found_entry->range)
                    printf("range: %s\n", found_entry->range);
                if (found_entry->guidance)
                    printf("guidance: %s\n", found_entry->guidance);
            }
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

/* Test seam: force the auto-answer state so non-interactive bug-repro tests
 * can drive prompt_yn() deterministically (1 => yes, -1 => no, 0 => prompt).
 * Not declared in cli.h (internal); the repro runner links cli.c directly and
 * carries an extern forward declaration. Production never calls this. */
void cbm_set_auto_answer_for_test(int value);
void cbm_set_auto_answer_for_test(int value) {
    g_auto_answer = value;
}

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

/* ── SHA-256 checksum verification ─────────────────────────────── */

#define SHA256_BUF_SIZE (CBM_SHA256_HEX_LEN + CLI_SKIP_ONE)
/* Minimum line length in checksums.txt: 64 hex + 2 spaces + 1 char filename */
#define CHECKSUM_LINE_MIN (CBM_SHA256_HEX_LEN + CLI_PAIR_LEN)

/* Compute the SHA-256 of a file in-process (no external hashing tool — those
 * differ per OS, may be absent, and mis-quote paths under cmd.exe). Writes a
 * 64-char hex digest + NUL to out. Returns 0 on success. Not static:
 * exercised directly by the self-update checksum regression test. */
int cbm_cli_sha256_file(const char *path, char *out, size_t out_size) {
    if (!path || !out || out_size < SHA256_BUF_SIZE) {
        return CLI_ERR;
    }
    FILE *fp = cbm_fopen(path, "rb");
    if (!fp) {
        return CLI_ERR;
    }
    cbm_sha256_ctx ctx;
    cbm_sha256_init(&ctx);
    unsigned char buf[CLI_BUF_1K];
    size_t n;
    while ((n = fread(buf, 1, sizeof(buf), fp)) > 0) {
        cbm_sha256_update(&ctx, buf, n);
    }
    int read_err = ferror(fp);
    fclose(fp);
    if (read_err) {
        return CLI_ERR;
    }
    uint8_t digest[CBM_SHA256_DIGEST_LEN];
    cbm_sha256_final(&ctx, digest);
    static const char hex[] = "0123456789abcdef";
    for (int i = 0; i < CBM_SHA256_DIGEST_LEN; i++) {
        out[i * 2] = hex[digest[i] >> 4];
        out[i * 2 + 1] = hex[digest[i] & 0x0f];
    }
    out[CBM_SHA256_HEX_LEN] = '\0';
    return 0;
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

static int cbm_cli_make_temp_file(char *out, size_t out_sz, const char *prefix) {
    if (!out || out_sz == 0 || !prefix || !prefix[0]) {
        errno = EINVAL;
        return CLI_ERR;
    }
    int n = snprintf(out, out_sz, "%s/%s_XXXXXX", cbm_tmpdir(), prefix);
    if (n < 0 || (size_t)n >= out_sz) {
        errno = ENAMETOOLONG;
        return CLI_ERR;
    }
    int fd = cbm_mkstemp_s(out, out_sz);
    if (fd < 0) {
        return CLI_ERR;
    }
    cbm_close_fd(fd);
    return 0;
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

/* ── Stop stale MCP server instances for a specific install target ─ */

static bool cbm_process_exe_path(unsigned long pid, char *out, size_t out_sz) {
    if (!out || out_sz == 0) {
        return false;
    }
    out[0] = '\0';
#ifdef _WIN32
    HANDLE hp = OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, FALSE, (DWORD)pid);
    if (!hp) {
        return false;
    }
    DWORD len = (DWORD)out_sz;
    BOOL ok = QueryFullProcessImageNameA(hp, 0, out, &len);
    CloseHandle(hp);
    if (!ok || len == 0 || len >= out_sz) {
        out[0] = '\0';
        return false;
    }
    cbm_normalize_path_sep(out);
    return true;
#elif defined(__APPLE__)
    int n = proc_pidpath((int)pid, out, (uint32_t)out_sz);
    if (n <= 0 || (size_t)n >= out_sz) {
        out[0] = '\0';
        return false;
    }
    out[n] = '\0';
    return true;
#elif defined(__linux__)
    char link_path[CLI_BUF_128];
    int n = snprintf(link_path, sizeof(link_path), "/proc/%lu/exe", pid);
    if (n < 0 || (size_t)n >= sizeof(link_path)) {
        return false;
    }
    ssize_t r = readlink(link_path, out, out_sz - CLI_SKIP_ONE);
    if (r <= 0 || (size_t)r >= out_sz) {
        out[0] = '\0';
        return false;
    }
    out[r] = '\0';
    char *deleted = strstr(out, " (deleted)");
    if (deleted) {
        *deleted = '\0';
    }
    return true;
#else
#endif
    return false;
}

static int cbm_stop_instances_for_target(const char *target_path) {
    if (!target_path || !target_path[0]) {
        return 0;
    }
    struct stat st;
    if (stat(target_path, &st) != 0) {
        return 0;
    }
    int killed = 0;
#ifdef _WIN32
    DWORD self = GetCurrentProcessId();
    HANDLE snap = CreateToolhelp32Snapshot(TH32CS_SNAPPROCESS, 0);
    if (snap == INVALID_HANDLE_VALUE) {
        return 0;
    }
    PROCESSENTRY32 pe;
    memset(&pe, 0, sizeof(pe));
    pe.dwSize = sizeof(pe);
    if (!Process32First(snap, &pe)) {
        CloseHandle(snap);
        return 0;
    }
    do {
        if (pe.th32ProcessID == self) {
            continue;
        }
        if (_stricmp(pe.szExeFile, "codebase-memory-mcp.exe") != 0) {
            continue;
        }
        char exe_path[CLI_BUF_1K];
        if (!cbm_process_exe_path((unsigned long)pe.th32ProcessID, exe_path, sizeof(exe_path))) {
            continue;
        }
        if (!cbm_same_file(exe_path, target_path)) {
            continue;
        }
        HANDLE hp = OpenProcess(PROCESS_TERMINATE, FALSE, pe.th32ProcessID);
        if (hp) {
            if (TerminateProcess(hp, 0)) {
                killed++;
            }
            CloseHandle(hp);
        }
    } while (Process32Next(snap, &pe));
    CloseHandle(snap);
#else
    pid_t self = getpid();
    FILE *fp = cbm_popen("pgrep -x codebase-memory-mcp", "r");
    if (!fp) {
        return 0;
    }
    char line[CLI_BUF_32];
    while (fgets(line, sizeof(line), fp)) {
        pid_t pid = (pid_t)strtol(line, NULL, CLI_STRTOL_BASE);
        if (pid <= 0 || pid == self) {
            continue;
        }
        char exe_path[CLI_BUF_1K];
        if (!cbm_process_exe_path((unsigned long)pid, exe_path, sizeof(exe_path))) {
            continue;
        }
        if (!cbm_same_file(exe_path, target_path)) {
            continue;
        }
        if (kill(pid, SIGTERM) == 0) {
            killed++;
        }
    }
    cbm_pclose(fp);
#endif
    return killed;
}

/* Download checksums.txt and verify the archive integrity.
 * Returns: 0 = verified OK, 1 = mismatch (FAIL), -1 = could not verify (warning). */
static int verify_download_checksum(const char *archive_path, const char *archive_name) {
    char checksum_file[CBM_PATH_MAX];
    if (cbm_cli_make_temp_file(checksum_file, sizeof(checksum_file), "cbm-checksums") != 0) {
        (void)fprintf(stderr,
                      "warning: could not create temporary checksum file — skipping verification\n");
        return CLI_ERR;
    }

    char dl_base_buf[CLI_BUF_512];
    const char *dl_base =
        cbm_safe_getenv("CBM_DOWNLOAD_URL", dl_base_buf, sizeof(dl_base_buf), NULL);
    char checksum_url[CLI_BUF_512];
    int url_len;
    if (dl_base && dl_base[0]) {
        url_len = snprintf(checksum_url, sizeof(checksum_url), "%s/checksums.txt", dl_base);
    } else {
        url_len = snprintf(checksum_url, sizeof(checksum_url), "%s",
                           "https://github.com/DeusData/codebase-memory-mcp/releases/latest/download/"
                           "checksums.txt");
    }
    if (url_len < 0 || (size_t)url_len >= sizeof(checksum_url)) {
        (void)fprintf(stderr, "warning: checksum URL too long — skipping verification\n");
        cbm_unlink(checksum_file);
        return CLI_ERR;
    }
    int rc = cbm_download_to_file_quiet(checksum_url, checksum_file);
    if (rc != 0) {
        (void)fprintf(stderr,
                      "warning: could not download checksums.txt — skipping verification\n");
        cbm_unlink(checksum_file);
        return CLI_ERR;
    }

    FILE *fp = fopen(checksum_file, "r");
    if (!fp) {
        cbm_unlink(checksum_file);
        return CLI_ERR;
    }

    char expected[SHA256_BUF_SIZE] = {0};
    char line[CLI_BUF_512];
    while (fgets(line, sizeof(line), fp)) {
        /* Format: <CBM_SZ_64-char sha256>  <filename>\n */
        if (strlen(line) > CHECKSUM_LINE_MIN && strstr(line, archive_name)) {
            memcpy(expected, line, CBM_SHA256_HEX_LEN);
            expected[CBM_SHA256_HEX_LEN] = '\0';
            break;
        }
    }
    (void)fclose(fp);
    cbm_unlink(checksum_file);

    if (expected[0] == '\0') {
        (void)fprintf(stderr, "warning: %s not found in checksums.txt\n", archive_name);
        return CLI_ERR;
    }

    char actual[SHA256_BUF_SIZE] = {0};
    if (cbm_cli_sha256_file(archive_path, actual, sizeof(actual)) != 0) {
        (void)fprintf(stderr, "error: could not compute checksum (sha256 tool unavailable)\n");
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
        {a->cursor, "Cursor"},
        {a->openclaw, "OpenClaw"},
        {a->kiro, "Kiro"},
        {a->junie, "Junie"},
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
/* ── Install plan recorder (issue #388) ────────────────────────────
 * When g_install_plan != NULL, the install path runs as a dry-run and each
 * write site records its planned target HERE — at the same point it would
 * perform the write — so the emitted plan cannot drift from actual install
 * behavior (it is the same code path with mutations disabled). */
typedef struct {
    char agent[CLI_BUF_32];
    char kind[CLI_BUF_32]; /* mcp_config | instructions | skills | hook */
    char path[CLI_BUF_1K];
} cbm_plan_entry_t;

typedef struct {
    cbm_plan_entry_t *items;
    int count;
    int cap;
} cbm_install_plan_t;

static cbm_install_plan_t *g_install_plan = NULL;

static void plan_record(const char *agent, const char *kind, const char *path) {
    if (!g_install_plan || !path || !path[0]) {
        return;
    }
    cbm_install_plan_t *pl = g_install_plan;
    if (pl->count >= pl->cap) {
        int ncap = pl->cap ? pl->cap * 2 : CLI_BUF_16;
        cbm_plan_entry_t *ni = realloc(pl->items, (size_t)ncap * sizeof(*ni));
        if (!ni) {
            return;
        }
        pl->items = ni;
        pl->cap = ncap;
    }
    cbm_plan_entry_t *e = &pl->items[pl->count++];
    snprintf(e->agent, sizeof(e->agent), "%s", agent);
    snprintf(e->kind, sizeof(e->kind), "%s", kind);
    snprintf(e->path, sizeof(e->path), "%s", path);
}

static void install_claude_code_config(const char *home, const char *binary_path, bool force,
                                       bool dry_run) {
    char config_dir[CLI_BUF_1K];
    cbm_claude_config_dir(home, config_dir, sizeof(config_dir));
    char user_root[CLI_BUF_1K];
    cbm_claude_user_root(home, user_root, sizeof(user_root));
    if (!config_dir[0] || !user_root[0]) {
        return;
    }

    char skills_dir[CLI_BUF_1K];
    if (!cbm_format_fits(skills_dir, sizeof(skills_dir), "%s/skills", config_dir)) {
        return;
    }

    /* Plan mode: record the planned writes and return without mutating (#388). */
    if (g_install_plan) {
        char p[CLI_BUF_1K];
        plan_record("Claude Code", "skills", skills_dir);
        if (cbm_format_fits(p, sizeof(p), "%s/.mcp.json", config_dir)) {
            plan_record("Claude Code", "mcp_config", p);
        }
        if (cbm_format_fits(p, sizeof(p), "%s/.claude.json", user_root)) {
            plan_record("Claude Code", "mcp_config", p);
        }
        if (cbm_format_fits(p, sizeof(p), "%s/settings.json", config_dir)) {
            plan_record("Claude Code", "mcp_config", p);
        }
        if (cbm_format_fits(p, sizeof(p), "%s/hooks/%s", config_dir, CMM_HOOK_GATE_SCRIPT)) {
            plan_record("Claude Code", "hook", p);
        }
        if (cbm_format_fits(p, sizeof(p), "%s/hooks/%s", config_dir,
                            CMM_SESSION_REMINDER_SCRIPT)) {
            plan_record("Claude Code", "hook", p);
        }
        if (cbm_format_fits(p, sizeof(p), "%s/hooks/%s", config_dir,
                            CMM_SUBAGENT_REMINDER_SCRIPT)) {
            plan_record("Claude Code", "hook", p);
        }
        return;
    }

    printf("Claude Code:\n");

    int skill_count = cbm_install_skills(skills_dir, force, dry_run);
    printf("  skills: %d installed\n", skill_count);

    if (cbm_remove_old_monolithic_skill(skills_dir, dry_run)) {
        printf("  removed old monolithic skill\n");
    }

    char mcp_path[CLI_BUF_1K];
    if (!cbm_format_fits(mcp_path, sizeof(mcp_path), "%s/.mcp.json", config_dir)) {
        return;
    }
    if (!dry_run) {
        cbm_install_editor_mcp(binary_path, mcp_path);
    }
    printf("  mcp: %s\n", mcp_path);

    char mcp_path2[CLI_BUF_1K];
    if (!cbm_format_fits(mcp_path2, sizeof(mcp_path2), "%s/.claude.json", user_root)) {
        return;
    }
    if (!dry_run) {
        cbm_install_editor_mcp(binary_path, mcp_path2);
    }
    printf("  mcp: %s\n", mcp_path2);

    char settings_path[CLI_BUF_1K];
    if (!cbm_format_fits(settings_path, sizeof(settings_path), "%s/settings.json", config_dir)) {
        return;
    }
    if (!dry_run) {
        cbm_upsert_claude_hooks(settings_path);
        cbm_install_hook_gate_script(home, binary_path);
        cbm_install_session_reminder_script(home);
        cbm_upsert_claude_session_hooks(settings_path);
        cbm_install_subagent_reminder_script(home);
        cbm_upsert_claude_subagent_hooks(settings_path);
    }
    printf("  hooks: PreToolUse (Grep/Glob search-graph augmenter, non-blocking)\n");
    printf("  hooks: SessionStart (MCP usage reminder on startup/resume/clear/compact)\n");
    printf("  hooks: SubagentStart (MCP usage reminder for subagents)\n");

    /* Migration nudge: when CLAUDE_CONFIG_DIR is set and a legacy ~/.claude tree
     * still exists, mention it so users can clean up stale artifacts. */
    if (home && home[0]) {
        char legacy_dir[CLI_BUF_1K];
        snprintf(legacy_dir, sizeof(legacy_dir), "%s/.claude", home);
        if (strcmp(legacy_dir, config_dir) != 0 && dir_exists(legacy_dir)) {
            (void)fprintf(stderr,
                          "  note: $CLAUDE_CONFIG_DIR=%s used; legacy %s still exists.\n"
                          "        Remove stale {skills,hooks,settings.json,.mcp.json} there if "
                          "no longer needed.\n",
                          config_dir, legacy_dir);
        }
    }
}

/* Install MCP config + optional instructions for a generic agent. */
static void install_generic_agent_config(const char *label, const char *binary_path,
                                         const char *config_path, const char *instr_path,
                                         bool dry_run,
                                         int (*install_mcp)(const char *, const char *)) {
    /* Plan mode: record planned writes, mutate nothing (#388). */
    if (g_install_plan) {
        plan_record(label, "mcp_config", config_path);
        if (instr_path) {
            plan_record(label, "instructions", instr_path);
        }
        return;
    }
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
    if (g_install_plan) {
        plan_record("Gemini CLI", "hook", cp); /* BeforeTool + SessionStart in settings.json */
        return;
    }
    if (!dry_run) {
        cbm_upsert_gemini_hooks(cp);
        cbm_upsert_gemini_session_hooks(cp);
    }
    printf("  hooks: BeforeTool + SessionStart (codebase-memory-mcp reminder)\n");
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
        /* Choose the hook target: if ~/.codex/hooks.json already exists, the
         * user manages Codex hooks via the JSON representation — write the
         * SessionStart reminder there instead of config.toml. Writing both
         * makes Codex warn about loading hooks from two representations (#570).
         * config.toml remains the mcp_config target above either way. */
        char hooks_json[CLI_BUF_1K];
        snprintf(hooks_json, sizeof(hooks_json), "%s/.codex/hooks.json", home);
        bool use_hooks_json = cbm_file_exists(hooks_json);
        const char *hook_target = use_hooks_json ? hooks_json : cp;
        if (g_install_plan) {
            plan_record("Codex CLI", "hook", hook_target);
        } else {
            if (!dry_run) {
                if (use_hooks_json) {
                    cbm_upsert_gemini_session_hooks(hooks_json);
                } else {
                    cbm_upsert_codex_hooks(cp);
                }
            }
            printf("  hooks: SessionStart (codebase-memory-mcp reminder)\n");
        }
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
        /* MCP config is the SHARED Antigravity config (CLI + IDE), not a
         * per-tool file (2026 unification). */
        snprintf(cp, sizeof(cp), "%s/.gemini/config/mcp_config.json", home);
        snprintf(ip, sizeof(ip), "%s/.gemini/antigravity-cli/AGENTS.md", home);
        if (!dry_run && !g_install_plan) {
            char cfg_dir[CLI_BUF_1K];
            snprintf(cfg_dir, sizeof(cfg_dir), "%s/.gemini/config", home);
            cbm_mkdir_p(cfg_dir, CLI_OCTAL_PERM);
        }
        install_generic_agent_config("Antigravity", binary_path, cp, ip, dry_run,
                                     cbm_upsert_antigravity_mcp);
        /* Antigravity CLI is Gemini-lineage and keeps a settings.json under
         * ~/.gemini/antigravity-cli/; install the SessionStart reminder there
         * using the shared Gemini hook JSON schema. */
        char sp[CLI_BUF_1K];
        snprintf(sp, sizeof(sp), "%s/.gemini/antigravity-cli/settings.json", home);
        if (g_install_plan) {
            plan_record("Antigravity", "hook", sp);
        } else {
            if (!dry_run) {
                cbm_upsert_gemini_session_hooks(sp);
            }
            printf("  hooks: SessionStart (codebase-memory-mcp reminder)\n");
        }
    }
    if (agents->aider) {
        char ip[CLI_BUF_1K];
        snprintf(ip, sizeof(ip), "%s/CONVENTIONS.md", home);
        if (g_install_plan) {
            plan_record("Aider", "instructions", ip);
        } else {
            printf("Aider:\n");
            if (!dry_run) {
                /* #1032: Aider cannot call MCP tools — CLI-form instructions. */
                cbm_upsert_instructions(ip, aider_instructions_content);
            }
            printf("  instructions: %s\n", ip);
        }
    }
}

/* Scan Code/User/profiles/ and install (or plan) a per-profile mcp.json for
 * each existing profile subdirectory, so VS Code profile users inherit the MCP
 * server without manual steps (#431). No-op when profiles/ is absent. */
static void install_vscode_profile_configs(const char *code_user, const char *binary_path,
                                           bool dry_run) {
    char profiles_dir[CLI_BUF_1K];
    snprintf(profiles_dir, sizeof(profiles_dir), "%s/profiles", code_user);
    cbm_dir_t *d = cbm_opendir(profiles_dir);
    if (!d) {
        return;
    }
    cbm_dirent_t *ent;
    while ((ent = cbm_readdir(d)) != NULL) {
        if (strcmp(ent->name, ".") == 0 || strcmp(ent->name, "..") == 0) {
            continue;
        }
        char profile_path[CLI_BUF_1K];
        snprintf(profile_path, sizeof(profile_path), "%s/%s", profiles_dir, ent->name);
        struct stat st;
        if (stat(profile_path, &st) != 0 || !S_ISDIR(st.st_mode)) {
            continue;
        }
        char cp[CLI_BUF_1K];
        snprintf(cp, sizeof(cp), "%s/mcp.json", profile_path);
        install_generic_agent_config("VS Code", binary_path, cp, NULL, dry_run,
                                     cbm_install_vscode_mcp);
    }
    cbm_closedir(d);
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
        char code_user[CLI_BUF_1K];
#ifdef __APPLE__
        snprintf(code_user, sizeof(code_user), "%s/Library/Application Support/Code/User", home);
#else
        snprintf(code_user, sizeof(code_user), "%s/Code/User", cbm_app_config_dir());
#endif
        char cp[CLI_BUF_1K];
        snprintf(cp, sizeof(cp), "%s/mcp.json", code_user);
        install_generic_agent_config("VS Code", binary_path, cp, NULL, dry_run,
                                     cbm_install_vscode_mcp);
        /* VS Code profiles each keep their own settings under
         * Code/User/profiles/<id>/. The default mcp.json above does NOT apply
         * to a named profile, so write/plan a per-profile mcp.json for every
         * existing profile directory (#431). */
        install_vscode_profile_configs(code_user, binary_path, dry_run);
    }
    if (agents->cursor) {
        char cp[CLI_BUF_1K];
        snprintf(cp, sizeof(cp), "%s/.cursor/mcp.json", home);
        install_generic_agent_config("Cursor", binary_path, cp, NULL, dry_run,
                                     cbm_install_editor_mcp);
    }
    if (agents->openclaw) {
        char cp[CLI_BUF_1K];
        snprintf(cp, sizeof(cp), "%s/.openclaw/openclaw.json", home);
        install_generic_agent_config("OpenClaw", binary_path, cp, NULL, dry_run,
                                     cbm_install_openclaw_mcp);
    }
    if (agents->kiro) {
        char cp[CLI_BUF_1K];
        char sd[CLI_BUF_1K];
        snprintf(cp, sizeof(cp), "%s/.kiro/settings/mcp.json", home);
        snprintf(sd, sizeof(sd), "%s/.kiro/settings", home);
        if (!dry_run) {
            cbm_mkdir_p(sd, CLI_OCTAL_PERM);
        }
        install_generic_agent_config("Kiro", binary_path, cp, NULL, dry_run,
                                     cbm_install_editor_mcp);
    }
    if (agents->junie) {
        char cp[CLI_BUF_1K];
        char sd[CLI_BUF_1K];
        snprintf(cp, sizeof(cp), "%s/.junie/mcp/mcp.json", home);
        snprintf(sd, sizeof(sd), "%s/.junie/mcp", home);
        if (!dry_run) {
            cbm_mkdir_p(sd, CLI_OCTAL_PERM);
        }
        install_generic_agent_config("Junie", binary_path, cp, NULL, dry_run, cbm_upsert_junie_mcp);
    }
}

static void cbm_install_agent_configs(const char *home, const char *binary_path, bool force,
                                      bool dry_run) {
    cbm_detected_agents_t agents = cbm_detect_agents(home);
    if (!g_install_plan) {
        print_detected_agents(&agents);
    }

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
        if (is_project_index_db_name(ent->name)) {
            count++;
        }
    }
    cbm_closedir(d);
    return count;
}

/* Handle pre-existing indexes during (re)install (#607).
 *
 * Returns 1 to proceed with the install, 0 to abort (user declined the
 * destructive reset prompt).
 *
 * Default (reset=false): PRESERVE the indexed graph. We do NOT delete any
 * .db. We print an honest message telling the user the indexes are kept and
 * that they should re-index after install to pick up this version's
 * extraction improvements. The old behaviour deleted every index here while
 * printing "must be rebuilt" and never rebuilt — silent, irrecoverable data
 * loss (#607). Deletion is NOT a schema requirement (the store uses CREATE
 * TABLE IF NOT EXISTS with no migrations); it only guarded against stale
 * content, which a re-index fixes without destroying anything.
 *
 * Opt-in (reset=true, via `install --reset-indexes`): keep the original
 * prompt-and-delete behaviour, with honest "Delete" wording.
 *
 * Not static: linked into the bug-repro test runner so repro_issue607.c can
 * assert the default path preserves the DB. It is intentionally NOT declared
 * in cli.h (internal helper); the test carries an extern forward declaration.
 */
int cbm_install_handle_existing_indexes(const char *home, bool reset, bool dry_run);
int cbm_install_handle_existing_indexes(const char *home, bool reset, bool dry_run) {
    int index_count = count_db_indexes(home);
    if (index_count <= 0) {
        return 1; /* nothing to handle, proceed */
    }

    if (!reset) {
        /* Default: preserve. Be honest — keep the indexes, advise re-index. */
        printf("Found %d existing index(es). Keeping them. After install, "
               "re-index to pick up this version's improvements:\n",
               index_count);
        cbm_list_indexes(home);
        printf("\n");
        return 1; /* proceed without deleting */
    }

    /* Opt-in reset (--reset-indexes): the original prompt-and-delete path. */
    printf("Found %d existing index(es):\n", index_count);
    cbm_list_indexes(home);
    printf("\n");
    if (!prompt_yn("Delete these indexes and continue with install?")) {
        printf("Install cancelled.\n");
        return 0; /* abort */
    }
    if (!dry_run) {
        int removed = cbm_remove_indexes(home);
        printf("Removed %d index(es).\n\n", removed);
    }
    return 1; /* proceed */
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

/* Build the agent.install.plan.v1 receipt (#388): a machine-readable list of
 * the config / instruction / hook files `install` WOULD write, produced by
 * running the real install dispatch in record-only mode (no mutation, no
 * network). Returns a heap JSON string (caller frees) or NULL. */
char *cbm_build_install_plan_json(const char *home, const char *binary_path) {
    if (!home || !binary_path) {
        return NULL;
    }

    /* Same code path as a real install, but mutations disabled and every write
     * site records into `plan` — so the receipt cannot drift from behavior. */
    cbm_install_plan_t plan = {0};
    g_install_plan = &plan;
    cbm_install_agent_configs(home, binary_path, false, true);
    g_install_plan = NULL;

    cbm_detected_agents_t det = cbm_detect_agents(home);
    struct {
        bool flag;
        const char *name;
    } names[] = {
        {det.claude_code, "claude-code"},
        {det.codex, "codex"},
        {det.gemini, "gemini"},
        {det.zed, "zed"},
        {det.opencode, "opencode"},
        {det.antigravity, "antigravity"},
        {det.aider, "aider"},
        {det.kilocode, "kilocode"},
        {det.vscode, "vscode"},
        {det.cursor, "cursor"},
        {det.openclaw, "openclaw"},
        {det.kiro, "kiro"},
        {det.junie, "junie"},
    };

    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);
    yyjson_mut_obj_add_str(doc, root, "type", "agent.install.plan.v1");

    yyjson_mut_val *agents = yyjson_mut_arr(doc);
    for (size_t i = 0; i < sizeof(names) / sizeof(names[0]); i++) {
        if (names[i].flag) {
            yyjson_mut_arr_add_str(doc, agents, names[i].name);
        }
    }
    yyjson_mut_obj_add_val(doc, root, "agents_detected", agents);

    yyjson_mut_val *configs = yyjson_mut_arr(doc);
    yyjson_mut_val *instrs = yyjson_mut_arr(doc);
    yyjson_mut_val *skill_dirs = yyjson_mut_arr(doc);
    yyjson_mut_val *hooks = yyjson_mut_arr(doc);
    for (int i = 0; i < plan.count; i++) {
        cbm_plan_entry_t *e = &plan.items[i];
        if (strcmp(e->kind, "mcp_config") == 0) {
            yyjson_mut_arr_add_strcpy(doc, configs, e->path);
        } else if (strcmp(e->kind, "skills") == 0) {
            yyjson_mut_arr_add_strcpy(doc, skill_dirs, e->path);
        } else if (strcmp(e->kind, "hook") == 0) {
            yyjson_mut_val *h = yyjson_mut_obj(doc);
            yyjson_mut_obj_add_strcpy(doc, h, "agent", e->agent);
            yyjson_mut_obj_add_strcpy(doc, h, "path", e->path);
            yyjson_mut_arr_add_val(hooks, h);
        } else {
            yyjson_mut_arr_add_strcpy(doc, instrs, e->path);
        }
    }
    yyjson_mut_obj_add_val(doc, root, "config_files_planned", configs);
    yyjson_mut_obj_add_val(doc, root, "instruction_files_planned", instrs);
    yyjson_mut_obj_add_val(doc, root, "skill_dirs_planned", skill_dirs);
    yyjson_mut_obj_add_val(doc, root, "hooks_planned", hooks);
    yyjson_mut_obj_add_bool(doc, root, "writes_started", false);
    yyjson_mut_obj_add_bool(doc, root, "network_after_install", false);
    yyjson_mut_obj_add_str(doc, root, "next_safe_command", "codebase-memory-mcp install -y");

    char *json = yyjson_mut_write(doc, YYJSON_WRITE_PRETTY, NULL);
    yyjson_mut_doc_free(doc);
    free(plan.items);
    return json; /* malloc'd; caller frees */
}

static bool cli_args_have_help(int argc, char **argv) {
    for (int i = 0; i < argc; i++) {
        if (strcmp(argv[i], "--help") == 0 || strcmp(argv[i], "-h") == 0) {
            return true;
        }
    }
    return false;
}

static void print_install_help(void) {
    puts("Usage: codebase-memory-mcp install [-y|-n] [--force] [--dry-run] [--plan]");
    puts("");
    puts("Install the current binary, MCP agent configs, skills, hooks, and PATH entries.");
    puts("");
    puts("Options:");
    puts("  -y, --yes   Answer yes to prompts");
    puts("  -n, --no    Answer no to prompts");
    puts("  --force     Overwrite existing installed files where supported");
    puts("  --dry-run   Show actions without modifying files");
    puts("  --plan      Print JSON install plan and do not modify files");
}

static void print_uninstall_help(void) {
    puts("Usage: codebase-memory-mcp uninstall [-y|-n] [--dry-run]");
    puts("");
    puts("Remove MCP agent configs, skills, hooks, indexes, and installed binary.");
    puts("");
    puts("Options:");
    puts("  -y, --yes   Answer yes to prompts");
    puts("  -n, --no    Answer no to prompts");
    puts("  --dry-run   Show actions without modifying files");
}

static void print_update_help(void) {
    puts("Usage: codebase-memory-mcp update [-y|-n] [--force] [--dry-run] [--standard|--ui]");
    puts("");
    puts("Download a release binary, replace the installed binary, and refresh agent configs.");
    puts("");
    puts("Options:");
    puts("  -y, --yes   Answer yes to prompts");
    puts("  -n, --no    Answer no to prompts");
    puts("  --force     Skip latest-version check");
    puts("  --dry-run   Show actions without downloading or modifying files");
    puts("  --standard  Select MCP-server-only binary without prompting");
    puts("  --ui        Select binary with embedded graph visualization without prompting");
}

int cbm_cmd_install(int argc, char **argv) {
    if (cli_args_have_help(argc, argv)) {
        print_install_help();
        return CLI_OK;
    }
    parse_auto_answer(argc, argv);
    bool dry_run = false;
    bool force = false;
    bool plan = false;
    bool reset_indexes = false;
    for (int i = 0; i < argc; i++) {
        if (strcmp(argv[i], "--dry-run") == 0) {
            dry_run = true;
        }
        if (strcmp(argv[i], "--force") == 0) {
            force = true;
        }
        if (strcmp(argv[i], "--plan") == 0) {
            plan = true;
        }
        /* Opt-in: delete existing indexes during install. Default preserves
         * the indexed graph (#607). Only this flag triggers deletion. */
        if (strcmp(argv[i], "--reset-indexes") == 0) {
            reset_indexes = true;
        }
    }

    const char *home = cbm_get_home_dir();
    if (!home) {
        (void)fprintf(stderr, "error: HOME not set (use USERPROFILE on Windows)\n");
        return CLI_TRUE;
    }

    /* --plan: emit the machine-readable install receipt and exit WITHOUT
     * mutating anything (no config writes, no index deletion, no network) so
     * an agent can inspect exactly what install would touch first (#388). */
    if (plan) {
        char self_path[CLI_BUF_1K] = {0};
        cbm_detect_self_path(self_path, sizeof(self_path), home);
        char *json = cbm_build_install_plan_json(home, self_path);
        if (!json) {
            (void)fprintf(stderr, "error: failed to build install plan\n");
            return CLI_TRUE;
        }
        printf("%s\n", json);
        free(json);
        return 0;
    }

    printf("codebase-memory-mcp install %s\n\n", CBM_VERSION);

    /* (#607) Default: preserve existing indexes. `--reset-indexes` opts into
     * the old prompt-and-delete behaviour. The helper returns 0 only when the
     * user declines the reset prompt, in which case we abort the install. */
    if (cbm_install_handle_existing_indexes(home, reset_indexes, dry_run) == 0) {
        return CLI_TRUE;
    }

    /* Step 1b: Place the running binary at the canonical install target.
     * Previously install only re-signed whatever was already at the target, so
     * `install --force` from a freshly built binary silently kept the OLD file
     * — operators ran stale code believing they had upgraded (#472). Copy the
     * running binary to ~/.local/bin (unless we ARE that file), then sign it. */
    char self_path[CLI_BUF_1K] = {0};
    cbm_detect_self_path(self_path, sizeof(self_path), home);

    char bin_target[CLI_BUF_1K];
#ifdef _WIN32
    if (!cbm_format_fits(bin_target, sizeof(bin_target),
                         "%s/.local/bin/codebase-memory-mcp.exe", home)) {
        (void)fprintf(stderr, "error: install target path is too long\n");
        return CLI_TRUE;
    }
#else
    if (!cbm_format_fits(bin_target, sizeof(bin_target), "%s/.local/bin/codebase-memory-mcp",
                         home)) {
        (void)fprintf(stderr, "error: install target path is too long\n");
        return CLI_TRUE;
    }
#endif

    /* Stop only server processes running this exact installed target. Matching
     * every process named codebase-memory-mcp can terminate unrelated agent
     * sessions when install is run against a fake HOME or alternate prefix. */
    if (!dry_run) {
        int killed = cbm_stop_instances_for_target(bin_target);
        if (killed > 0) {
            printf("Stopped %d running MCP server instance(s).\n\n", killed);
        }
    }

    if (!cbm_same_file(self_path, bin_target)) {
        struct stat tgt_st;
        bool target_exists = (stat(bin_target, &tgt_st) == 0);
        bool do_copy = !target_exists || force;
        if (target_exists && !force) {
            printf("A different binary already exists at:\n  %s\n", bin_target);
            if (prompt_yn("Replace it with the binary you ran install from?")) {
                do_copy = true;
                force = true; /* user approved replacement for this run */
            } else {
                printf("Keeping existing binary; configs will point at it.\n\n");
            }
        }
        if (do_copy) {
            char bin_dir[CLI_BUF_1K];
            if (!cbm_format_fits(bin_dir, sizeof(bin_dir), "%s/.local/bin", home)) {
                (void)fprintf(stderr, "error: install bin directory path is too long\n");
                return CLI_TRUE;
            }
            if (dry_run) {
                printf("Would install binary -> %s\n\n", bin_target);
            } else {
                if (!cbm_mkdir_p(bin_dir, CLI_OCTAL_PERM)) {
                    (void)fprintf(stderr, "error: failed to create %s\n", bin_dir);
                    return CLI_TRUE;
                }
                if (cbm_copy_binary_to_target(self_path, bin_target) != 0) {
                    (void)fprintf(stderr, "error: failed to copy binary to %s\n", bin_target);
                    return CLI_TRUE;
                }
                printf("Installed binary -> %s\n\n", bin_target);
            }
        }
    }

    /* Step 1d: macOS ad-hoc signing of the installed binary. A freshly
     * clang-built arm64 binary is linker-signed (flags=0x20002) and gets
     * Killed:9 when spawned by an MCP host; re-signing ad-hoc (flags=0x2)
     * makes it launchable. Sign the target, not whatever the operator ran. */
#ifdef __APPLE__
    if (!dry_run) {
        struct stat sign_st;
        if (stat(bin_target, &sign_st) == 0) {
            if (cbm_macos_adhoc_sign(bin_target) != 0) {
                (void)fprintf(
                    stderr, "warning: ad-hoc signing failed — binary may not run on macOS arm64\n");
            }
        }
    }
#endif

    /* Step 3: Install/refresh all agent configs, pointing at the install target. */
    cbm_install_agent_configs(home, bin_target, force, dry_run);

    /* Step 4: Ensure PATH */
    char bin_dir[CLI_BUF_1K];
    if (!cbm_format_fits(bin_dir, sizeof(bin_dir), "%s/.local/bin", home)) {
        (void)fprintf(stderr, "error: install bin directory path is too long\n");
        return CLI_TRUE;
    }
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
    char config_dir[CLI_BUF_1K];
    cbm_claude_config_dir(home, config_dir, sizeof(config_dir));
    char user_root[CLI_BUF_1K];
    cbm_claude_user_root(home, user_root, sizeof(user_root));
    if (!config_dir[0] || !user_root[0]) {
        return;
    }

    char skills_dir[CLI_BUF_1K];
    if (!cbm_format_fits(skills_dir, sizeof(skills_dir), "%s/skills", config_dir)) {
        return;
    }
    int removed = cbm_remove_skills(skills_dir, dry_run);
    printf("Claude Code: removed %d skill(s)\n", removed);

    char mcp_path[CLI_BUF_1K];
    if (!cbm_format_fits(mcp_path, sizeof(mcp_path), "%s/.mcp.json", config_dir)) {
        return;
    }
    if (!dry_run) {
        cbm_remove_editor_mcp(mcp_path);
    }
    printf("  removed MCP config entry\n");

    char mcp_path2[CLI_BUF_1K];
    if (!cbm_format_fits(mcp_path2, sizeof(mcp_path2), "%s/.claude.json", user_root)) {
        return;
    }
    if (!dry_run) {
        cbm_remove_editor_mcp(mcp_path2);
    }

    char settings_path[CLI_BUF_1K];
    if (!cbm_format_fits(settings_path, sizeof(settings_path), "%s/settings.json", config_dir)) {
        return;
    }
    if (!dry_run) {
        cbm_remove_claude_hooks(settings_path);
        cbm_remove_claude_session_hooks(settings_path);
        cbm_remove_claude_subagent_hooks(settings_path);
    }
    printf("  removed PreToolUse + SessionStart + SubagentStart hooks\n");
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
        cbm_remove_gemini_session_hooks(cp);
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
        if (!dry_run) {
            cbm_remove_codex_hooks(cp);
        }
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
        snprintf(cp, sizeof(cp), "%s/.gemini/config/mcp_config.json", home);
        snprintf(ip, sizeof(ip), "%s/.gemini/antigravity-cli/AGENTS.md", home);
        uninstall_agent_mcp_instr((mcp_uninstall_args_t){"Antigravity", cp, ip}, dry_run,
                                  cbm_remove_antigravity_mcp);
        if (!dry_run) {
            char sp[CLI_BUF_1K];
            snprintf(sp, sizeof(sp), "%s/.gemini/antigravity-cli/settings.json", home);
            cbm_remove_gemini_session_hooks(sp);
        }
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
    if (agents->cursor) {
        char cp[CLI_BUF_1K];
        snprintf(cp, sizeof(cp), "%s/.cursor/mcp.json", home);
        uninstall_agent_mcp_instr((mcp_uninstall_args_t){"Cursor", cp, NULL}, dry_run,
                                  cbm_remove_editor_mcp);
    }
    if (agents->openclaw) {
        char cp[CLI_BUF_1K];
        snprintf(cp, sizeof(cp), "%s/.openclaw/openclaw.json", home);
        uninstall_agent_mcp_instr((mcp_uninstall_args_t){"OpenClaw", cp, NULL}, dry_run,
                                  cbm_remove_openclaw_mcp);
    }
    if (agents->kiro) {
        char cp[CLI_BUF_1K];
        snprintf(cp, sizeof(cp), "%s/.kiro/settings/mcp.json", home);
        uninstall_agent_mcp_instr((mcp_uninstall_args_t){"Kiro", cp, NULL}, dry_run,
                                  cbm_remove_editor_mcp);
    }
    if (agents->junie) {
        char cp[CLI_BUF_1K];
        snprintf(cp, sizeof(cp), "%s/.junie/mcp/mcp.json", home);
        uninstall_agent_mcp_instr((mcp_uninstall_args_t){"Junie", cp, NULL}, dry_run,
                                  cbm_remove_junie_mcp);
    }
}

int cbm_cmd_uninstall(int argc, char **argv) {
    if (cli_args_have_help(argc, argv)) {
        print_uninstall_help();
        return CLI_OK;
    }
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
static int build_update_url(char *url, int url_sz, const char *os, const char *arch,
                             const char *ext, bool want_ui) {
    char base_url_buf[CLI_BUF_512];
    const char *base_url =
        cbm_safe_getenv("CBM_DOWNLOAD_URL", base_url_buf, sizeof(base_url_buf), NULL);
    if (!base_url || !base_url[0]) {
        base_url = "https://github.com/DeusData/codebase-memory-mcp/releases/latest/download";
    }
    /* Linux ships a fully-static "-portable" build; the standard linux binary
     * dynamically links glibc 2.38+ and fails on older distros. macOS/Windows
     * have no such variant. Keep in sync with install.sh / install.js / pypi
     * _cli.py. */
    const char *portable = (strcmp(os, "linux") == 0) ? "-portable" : "";
    int n = snprintf(url, (size_t)url_sz, "%s/codebase-memory-mcp-%s%s-%s%s.%s", base_url,
                     want_ui ? "ui-" : "", os, arch, portable, ext);
    return (n >= 0 && n < url_sz) ? 0 : CLI_ERR;
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
    char tmp_archive[CBM_PATH_MAX];
    if (cbm_cli_make_temp_file(tmp_archive, sizeof(tmp_archive), "cbm-update") != 0) {
        (void)fprintf(stderr, "error: cannot create temporary update archive path\n");
        return CLI_TRUE;
    }

    int rc = cbm_download_to_file(url, tmp_archive);
    if (rc != 0) {
        (void)fprintf(stderr, "error: download failed (exit %d)\n", rc);
        cbm_unlink(tmp_archive);
        return CLI_TRUE;
    }

    char archive_name[CLI_BUF_256];
    /* Must match build_update_url: linux uses the static "-portable" asset. */
    const char *portable = (strcmp(os, "linux") == 0) ? "-portable" : "";
    snprintf(archive_name, sizeof(archive_name), "codebase-memory-mcp-%s%s-%s%s.%s",
             want_ui ? "ui-" : "", os, arch, portable, ext);
    /* Fail closed: install only a positively-verified download. A mismatch,
     * a missing checksum entry, or an unavailable hash tool (crc != 0) all
     * abort rather than install an unverified binary. */
    int crc = verify_download_checksum(tmp_archive, archive_name);
    if (crc != 0) {
        (void)fprintf(stderr, "error: refusing to install an unverified download\n");
        cbm_unlink(tmp_archive);
        return CLI_TRUE;
    }

    int killed = cbm_stop_instances_for_target(bin_dest);
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
            tag = cbm_strdup(slash);
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
    if (cli_args_have_help(argc, argv)) {
        print_update_help();
        return CLI_OK;
    }
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
    if (build_update_url(url, sizeof(url), os, arch, ext, want_ui) != 0) {
        (void)fprintf(stderr, "error: update download URL is too long\n");
        return CLI_TRUE;
    }

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
    if (!cbm_format_fits(bin_dest, sizeof(bin_dest), "%s/.local/bin/codebase-memory-mcp.exe",
                         home)) {
        (void)fprintf(stderr, "error: update target path is too long\n");
        return CLI_TRUE;
    }
#else
    if (!cbm_format_fits(bin_dest, sizeof(bin_dest), "%s/.local/bin/codebase-memory-mcp",
                         home)) {
        (void)fprintf(stderr, "error: update target path is too long\n");
        return CLI_TRUE;
    }
#endif
    char bin_dir[CLI_BUF_1K];
    if (!cbm_format_fits(bin_dir, sizeof(bin_dir), "%s/.local/bin", home)) {
        (void)fprintf(stderr, "error: update bin directory path is too long\n");
        return CLI_TRUE;
    }
    if (!cbm_mkdir_p(bin_dir, CLI_OCTAL_PERM)) {
        (void)fprintf(stderr, "error: failed to create %s\n", bin_dir);
        return CLI_TRUE;
    }

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

/* ── CLI tool arguments (flags / --args-file / --help) ────────────── */

/* Flag-name normalization: kebab-case CLI flags map to snake_case JSON keys
 * (--name-pattern -> name_pattern). In-place; buffer is NUL-terminated. */
static void cli_kebab_to_snake(char *s) {
    for (; *s; s++) {
        if (*s == '-') {
            *s = '_';
        }
    }
}

/* snake_case JSON key -> kebab-case flag name (for --help display). In-place. */
static void cli_snake_to_kebab(char *s) {
    for (; *s; s++) {
        if (*s == '_') {
            *s = '-';
        }
    }
}

/* Heap-format a one-argument error message for *err_out. Caller frees. */
static char *cli_heap_msgf(const char *fmt, const char *arg) {
    char buf[CLI_BUF_512];
    snprintf(buf, sizeof(buf), fmt, arg);
    return cbm_strdup(buf);
}

/* Levenshtein distance for near-miss flag suggestions (two-row DP; inputs
 * are schema property names, well under the buffer sizes used here). */
static int cli_edit_distance(const char *a, const char *b) {
    enum { CLI_ED_MAX = 128 };
    size_t la = strlen(a);
    size_t lb = strlen(b);
    if (la >= CLI_ED_MAX || lb >= CLI_ED_MAX) {
        return CLI_ED_MAX;
    }
    int prev[CLI_ED_MAX + 1];
    int cur[CLI_ED_MAX + 1];
    for (size_t j = 0; j <= lb; j++) {
        prev[j] = (int)j;
    }
    for (size_t i = 1; i <= la; i++) {
        cur[0] = (int)i;
        for (size_t j = 1; j <= lb; j++) {
            int cost = (a[i - 1] == b[j - 1]) ? 0 : 1;
            int del = prev[j] + 1;
            int ins = cur[j - 1] + 1;
            int sub = prev[j - 1] + cost;
            int m = del < ins ? del : ins;
            cur[j] = m < sub ? m : sub;
        }
        memcpy(prev, cur, (lb + 1) * sizeof(int));
    }
    return prev[lb];
}

/* Closest schema property to `key` for a "did you mean" suggestion, or NULL
 * when nothing is plausibly near (distance > half the key length, min 2). */
static const char *cli_closest_prop(yyjson_val *props, const char *key) {
    const char *best = NULL;
    int best_d = 0;
    size_t idx;
    size_t max;
    yyjson_val *k;
    yyjson_val *v;
    yyjson_obj_foreach(props, idx, max, k, v) {
        const char *name = yyjson_get_str(k);
        if (!name) {
            continue;
        }
        int d = cli_edit_distance(key, name);
        if (!best || d < best_d) {
            best = name;
            best_d = d;
        }
    }
    int limit = (int)(strlen(key) / 2);
    if (limit < 2) {
        limit = 2;
    }
    return (best && best_d <= limit) ? best : NULL;
}

/* True if the schema's required[] array contains `key`. */
static bool cli_schema_required_has(yyjson_val *required, const char *key) {
    if (!required || !yyjson_is_arr(required)) {
        return false;
    }
    size_t idx;
    size_t max;
    yyjson_val *v;
    yyjson_arr_foreach(required, idx, max, v) {
        if (yyjson_is_str(v) && strcmp(yyjson_get_str(v), key) == 0) {
            return true;
        }
    }
    return false;
}

/* Look up a property's JSON-schema "type" string (string/integer/number/
 * boolean/array). Returns NULL when the schema or property is unknown — the
 * caller then treats the value as a plain string. */
static const char *cli_schema_type(yyjson_val *props, const char *key) {
    if (!props || !yyjson_is_obj(props)) {
        return NULL;
    }
    yyjson_val *p = yyjson_obj_get(props, key);
    if (!p || !yyjson_is_obj(p)) {
        return NULL;
    }
    yyjson_val *t = yyjson_obj_get(p, "type");
    return (t && yyjson_is_str(t)) ? yyjson_get_str(t) : NULL;
}

/* Append a typed value to the output object under `key`. For array-typed
 * properties, repeated flags accumulate into a single JSON array. */
static void cli_add_typed(yyjson_mut_doc *out, yyjson_mut_val *obj, const char *key,
                          const char *type, const char *value, bool have_value) {
    if (type && strcmp(type, "array") == 0) {
        yyjson_mut_val *arr = yyjson_mut_obj_get(obj, key);
        if (!arr || !yyjson_mut_is_arr(arr)) {
            arr = yyjson_mut_arr(out);
            yyjson_mut_obj_add(obj, yyjson_mut_strcpy(out, key), arr);
        }
        yyjson_mut_arr_add_strcpy(out, arr, have_value ? value : "");
        return;
    }

    yyjson_mut_val *vv;
    if (type && strcmp(type, "boolean") == 0) {
        bool b = !have_value || strcmp(value, "true") == 0 || strcmp(value, "1") == 0 ||
                 strcmp(value, "yes") == 0;
        vv = yyjson_mut_bool(out, b);
    } else if (type && strcmp(type, "integer") == 0) {
        char *endp = NULL;
        const char *v = have_value ? value : "";
        long n = strtol(v, &endp, CLI_STRTOL_BASE);
        vv = (endp && endp != v && *endp == '\0') ? yyjson_mut_int(out, (int64_t)n)
                                                  : yyjson_mut_strcpy(out, v);
    } else if (type && strcmp(type, "number") == 0) {
        char *endp = NULL;
        const char *v = have_value ? value : "";
        double d = strtod(v, &endp);
        vv = (endp && endp != v && *endp == '\0') ? yyjson_mut_real(out, d)
                                                  : yyjson_mut_strcpy(out, v);
    } else {
        /* string or unknown type */
        vv = yyjson_mut_strcpy(out, have_value ? value : "");
    }
    yyjson_mut_obj_add(obj, yyjson_mut_strcpy(out, key), vv);
}

char *cbm_cli_build_args_json(const char *tool_name, int argc, char **argv, char **err_out) {
    if (err_out) {
        *err_out = NULL;
    }

    /* The tool's input_schema (may be NULL for an unknown tool — then every
     * value is treated as a string). Static lifetime; do not free. */
    const char *schema_str = cbm_mcp_tool_input_schema(tool_name);
    yyjson_doc *schema_doc = NULL;
    yyjson_val *props = NULL;
    if (schema_str) {
        schema_doc = yyjson_read(schema_str, strlen(schema_str), 0);
        if (schema_doc) {
            props = yyjson_obj_get(yyjson_doc_get_root(schema_doc), "properties");
        }
    }

    yyjson_mut_doc *out = yyjson_mut_doc_new(NULL);
    if (!out) {
        if (schema_doc) {
            yyjson_doc_free(schema_doc);
        }
        return NULL;
    }
    yyjson_mut_val *obj = yyjson_mut_obj(out);
    yyjson_mut_doc_set_root(out, obj);

    bool ok = true;
    for (int i = 0; i < argc && ok; i++) {
        const char *arg = argv[i];
        if (strcmp(arg, "--") == 0) {
            break; /* end of flag parsing */
        }
        if (strncmp(arg, "--", CLI_PAIR_LEN) != 0) {
            if (err_out) {
                *err_out = cli_heap_msgf("unexpected argument '%s' (expected --flag value)", arg);
            }
            ok = false;
            break;
        }

        const char *body = arg + CLI_PAIR_LEN; /* skip leading "--" */
        const char *eq = strchr(body, '=');
        char key[CLI_BUF_256];
        const char *value = NULL;
        bool have_value = false;

        if (eq) {
            /* --key=value : split on the FIRST '='; value may contain '='/spaces. */
            size_t klen = (size_t)(eq - body);
            if (klen >= sizeof(key)) {
                klen = sizeof(key) - CLI_SKIP_ONE;
            }
            memcpy(key, body, klen);
            key[klen] = '\0';
            value = eq + CLI_SKIP_ONE;
            have_value = true;
        } else {
            snprintf(key, sizeof(key), "%s", body);
            /* Consume the next token as the value unless it is itself a flag
             * (then this is a bare boolean/string flag). */
            if (i + CLI_SKIP_ONE < argc &&
                strncmp(argv[i + CLI_SKIP_ONE], "--", CLI_PAIR_LEN) != 0) {
                value = argv[i + CLI_SKIP_ONE];
                have_value = true;
                i++;
            }
        }

        cli_kebab_to_snake(key);
        const char *type = cli_schema_type(props, key);

        /* Unknown flag for a known tool: reject loudly (#997). Silently
         * typing it as a string ships it as an ignored JSON arg — the
         * server applies its default and the caller gets silently-wrong
         * output (e.g. `trace_path --max-depth 1` traced at depth 3). */
        if (props && !type) {
            char kebab_key[CLI_BUF_256];
            snprintf(kebab_key, sizeof(kebab_key), "%s", key);
            cli_snake_to_kebab(kebab_key);
            const char *close = cli_closest_prop(props, key);
            char suggestion[CLI_BUF_256] = "";
            if (close) {
                char close_kebab[CLI_BUF_256];
                snprintf(close_kebab, sizeof(close_kebab), "%s", close);
                cli_snake_to_kebab(close_kebab);
                snprintf(suggestion, sizeof(suggestion), " (did you mean --%s?)", close_kebab);
            }
            if (err_out) {
                char buf[CLI_BUF_512];
                snprintf(buf, sizeof(buf),
                         "unknown flag --%s for this tool%s — run 'cli %s --help' for the "
                         "supported flags",
                         kebab_key, suggestion, tool_name);
                *err_out = cbm_strdup(buf);
            }
            ok = false;
            break;
        }

        if (type && strcmp(type, "array") == 0 && !have_value) {
            if (err_out) {
                *err_out = cli_heap_msgf("flag --%s requires a value", key);
            }
            ok = false;
            break;
        }

        cli_add_typed(out, obj, key, type, value, have_value);
    }

    char *result = NULL;
    if (ok) {
        size_t len = 0;
        result = yyjson_mut_write(out, 0, &len); /* malloc'd; caller frees */
    }

    yyjson_mut_doc_free(out);
    if (schema_doc) {
        yyjson_doc_free(schema_doc);
    }
    return result;
}

int cbm_cli_print_tool_help(const char *tool_name) {
    const char *schema_str = cbm_mcp_tool_input_schema(tool_name);
    if (!schema_str) {
        return CLI_ERR;
    }

    yyjson_doc *doc = yyjson_read(schema_str, strlen(schema_str), 0);
    yyjson_val *root = doc ? yyjson_doc_get_root(doc) : NULL;
    yyjson_val *props = root ? yyjson_obj_get(root, "properties") : NULL;
    yyjson_val *required = root ? yyjson_obj_get(root, "required") : NULL;

    printf("Usage:\n");
    printf("  codebase-memory-mcp cli %s --flag value [--flag2 value2 ...]\n", tool_name);
    printf("  codebase-memory-mcp cli %s --args-file <path-to-json>\n", tool_name);
    printf("  echo '<json>' | codebase-memory-mcp cli %s\n", tool_name);
    printf("  codebase-memory-mcp cli %s '<raw-json-args>'\n", tool_name);

    printf("\nFlags:\n");
    if (props && yyjson_is_obj(props)) {
        yyjson_obj_iter iter;
        yyjson_obj_iter_init(props, &iter);
        yyjson_val *pkey;
        while ((pkey = yyjson_obj_iter_next(&iter)) != NULL) {
            yyjson_val *pval = yyjson_obj_iter_get_val(pkey);
            const char *name = yyjson_get_str(pkey);
            if (!name) {
                continue;
            }
            const char *type = "string";
            const char *desc = "";
            if (yyjson_is_obj(pval)) {
                yyjson_val *t = yyjson_obj_get(pval, "type");
                if (t && yyjson_is_str(t)) {
                    type = yyjson_get_str(t);
                }
                yyjson_val *d = yyjson_obj_get(pval, "description");
                if (d && yyjson_is_str(d)) {
                    desc = yyjson_get_str(d);
                }
            }
            char flag[CLI_BUF_256];
            snprintf(flag, sizeof(flag), "%s", name);
            cli_snake_to_kebab(flag);
            bool req = cli_schema_required_has(required, name);
            printf("  --%s <%s>%s", flag, type, req ? " [required]" : "");
            if (desc[0]) {
                printf("  %s", desc);
            }
            printf("\n");
        }
    }

    if (doc) {
        yyjson_doc_free(doc);
    }
    return CLI_OK;
}
