/*
 * cli.c — CLI subcommand handlers for install, uninstall, update, version.
 *
 * Port of Go cmd/codebase-memory-mcp/ install/update logic.
 * All functions accept explicit paths for testability.
 */
#include "cli/cli.h"
#include "foundation/compat.h"
#include "foundation/str_util.h"

// the correct standard headers are included below but clang-tidy doesn't map them.
#include <ctype.h>
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

/* ── Constants ────────────────────────────────────────────────── */

/* Directory permissions: rwxr-x--- */
#define DIR_PERMS 0750

/* Decompression buffer cap (500 MB) */
#define DECOMPRESS_MAX_BYTES ((size_t)500 * 1024 * 1024)

/* Tar header field offsets */
#define TAR_NAME_LEN 101    /* filename field: bytes 0-99 + NUL */
#define TAR_SIZE_OFFSET 124 /* octal size field offset */
#define TAR_SIZE_LEN 13     /* octal size field: bytes 124-135 + NUL */
#define TAR_TYPE_OFFSET 156 /* type flag byte */
#define TAR_BINARY_NAME "codebase-memory-mcp"
#define TAR_BINARY_NAME_LEN 19
#define TAR_BLOCK_SIZE 512 /* tar record alignment */
#define TAR_BLOCK_MASK 511 /* TAR_BLOCK_SIZE - 1 */

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
static int parse_semver(const char *v, int out[3]) {
    out[0] = out[1] = out[2] = 0;
    /* Skip v prefix */
    if (*v == 'v' || *v == 'V') {
        v++;
    }

    int count = 0;
    while (*v && count < 3) {
        if (*v == '-') {
            break; /* stop at pre-release suffix */
        }
        char *endptr;
        long val = strtol(v, &endptr, 10);
        out[count++] = (int)val;
        if (*endptr == '.') {
            v = endptr + 1;
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
    int pa[3];
    int pb[3];
    parse_semver(a, pa);
    parse_semver(b, pb);

    for (int i = 0; i < 3; i++) {
        if (pa[i] != pb[i]) {
            return pa[i] - pb[i];
        }
    }

    /* Same base version — non-dev beats dev */
    bool a_pre = has_prerelease(a);
    bool b_pre = has_prerelease(b);
    if (a_pre && !b_pre) {
        return -1;
    }
    if (!a_pre && b_pre) {
        return 1;
    }
    return 0;
}

/* ── Shell RC detection ───────────────────────────────────────── */

const char *cbm_detect_shell_rc(const char *home_dir) {
    static char buf[512];
    if (!home_dir || !home_dir[0]) {
        return "";
    }

    // NOLINTNEXTLINE(concurrency-mt-unsafe)
    const char *shell = getenv("SHELL");
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

const char *cbm_find_cli(const char *name, const char *home_dir) {
    static char buf[512];
    if (!name || !name[0]) {
        return "";
    }

    /* Check PATH first */
    // NOLINTNEXTLINE(concurrency-mt-unsafe)
    const char *path_env = getenv("PATH");
    if (path_env) {
        char path_copy[4096];
        snprintf(path_copy, sizeof(path_copy), "%s", path_env);
        char *saveptr;
        // NOLINTNEXTLINE(misc-include-cleaner) — strtok_r provided by standard header
        char *dir = strtok_r(path_copy, ":", &saveptr);
        while (dir) {
            snprintf(buf, sizeof(buf), "%s/%s", dir, name);
            struct stat st;
            // NOLINTNEXTLINE(misc-include-cleaner) — S_IXUSR provided by standard header
            if (stat(buf, &st) == 0 && (st.st_mode & S_IXUSR)) {
                return buf;
            }
            dir = strtok_r(NULL, ":", &saveptr);
        }
    }

    /* Check common install locations */
    if (home_dir && home_dir[0]) {
        const char *candidates[] = {
            "/usr/local/bin/%s",
            NULL, /* filled dynamically */
            NULL,
            NULL,
            NULL,
        };
        char paths[5][512];
        snprintf(paths[0], sizeof(paths[0]), "/usr/local/bin/%s", name);
        snprintf(paths[1], sizeof(paths[1]), "%s/.npm/bin/%s", home_dir, name);
        snprintf(paths[2], sizeof(paths[2]), "%s/.local/bin/%s", home_dir, name);
        snprintf(paths[3], sizeof(paths[3]), "%s/.cargo/bin/%s", home_dir, name);
#ifdef __APPLE__
        snprintf(paths[4], sizeof(paths[4]), "/opt/homebrew/bin/%s", name);
#else
        paths[4][0] = '\0';
#endif
        (void)candidates;

        for (int i = 0; i < 5; i++) {
            if (!paths[i][0]) {
                continue;
            }
            struct stat st;
            if (stat(paths[i], &st) == 0) {
                snprintf(buf, sizeof(buf), "%s", paths[i]);
                return buf;
            }
        }
    }

    return "";
}

/* ── File utilities ───────────────────────────────────────────── */

int cbm_copy_file(const char *src, const char *dst) {
    FILE *in = fopen(src, "rb");
    if (!in) {
        return -1;
    }

    FILE *out = fopen(dst, "wb");
    if (!out) {
        (void)fclose(in);
        return -1;
    }

    char buf[8192];
    int err = 0;
    while (!feof(in) && !ferror(in)) {
        size_t n = fread(buf, 1, sizeof(buf), in);
        if (n == 0) {
            break;
        }
        if (fwrite(buf, 1, n, out) != n) {
            err = 1;
            break;
        }
    }

    if (err || ferror(in)) {
        (void)fclose(in);
        (void)fclose(out);
        return -1;
    }

    (void)fclose(in);
    int rc = fclose(out);
    return rc == 0 ? 0 : -1;
}

/* ── Skill file content (embedded) ────────────────────────────── */

static const char skill_exploring_content[] =
    "---\n"
    "name: codebase-memory-exploring\n"
    "description: Codebase knowledge graph expert. ALWAYS invoke this skill when the user "
    "explores code, searches for functions/classes/routes, asks about architecture, or needs "
    "codebase orientation. Do not use Grep, Glob, or file search directly — use "
    "codebase-memory-mcp search_graph and get_architecture first.\n"
    "---\n"
    "\n"
    "# Codebase Exploration\n"
    "\n"
    "Use codebase-memory-mcp tools to explore the codebase:\n"
    "\n"
    "## Workflow\n"
    "1. `get_graph_schema` — understand what node/edge types exist\n"
    "2. `search_graph` — find functions, classes, routes by pattern\n"
    "3. `get_code_snippet` — read specific function implementations\n"
    "4. `get_architecture` — get high-level project summary\n"
    "\n"
    "## Tips\n"
    "- Use `search_graph(name_pattern=\".*Pattern.*\")` for fuzzy matching\n"
    "- Use `search_graph(label=\"Route\")` to find HTTP routes\n"
    "- Use `search_graph(label=\"Function\", file_pattern=\"*.go\")` to scope by language\n";

static const char skill_tracing_content[] =
    "---\n"
    "name: codebase-memory-tracing\n"
    "description: Call chain and dependency expert. ALWAYS invoke this skill when the user "
    "asks who calls a function, what a function calls, needs impact analysis, or traces "
    "dependencies. Do not grep for function names directly — use codebase-memory-mcp "
    "trace_call_path first.\n"
    "---\n"
    "\n"
    "# Call Tracing & Impact Analysis\n"
    "\n"
    "Use codebase-memory-mcp tools to trace call paths:\n"
    "\n"
    "## Workflow\n"
    "1. `search_graph(name_pattern=\".*FuncName.*\")` — find exact function name\n"
    "2. `trace_call_path(function_name=\"FuncName\", direction=\"both\")` — trace callers + "
    "callees\n"
    "3. `detect_changes` — find what changed and assess risk_labels\n"
    "\n"
    "## Direction Options\n"
    "- `inbound` — who calls this function?\n"
    "- `outbound` — what does this function call?\n"
    "- `both` — full context (callers + callees)\n";

static const char skill_quality_content[] =
    "---\n"
    "name: codebase-memory-quality\n"
    "description: Code quality analysis expert. ALWAYS invoke this skill when the user asks "
    "about dead code, unused functions, complexity, refactor candidates, or cleanup "
    "opportunities. Do not search files manually — use codebase-memory-mcp search_graph "
    "with degree filters first.\n"
    "---\n"
    "\n"
    "# Code Quality Analysis\n"
    "\n"
    "Use codebase-memory-mcp tools for quality analysis:\n"
    "\n"
    "## Dead Code Detection\n"
    "- `search_graph(max_degree=0, exclude_entry_points=true)` — find unreferenced functions\n"
    "- `search_graph(max_degree=0, label=\"Function\")` — unreferenced functions only\n"
    "\n"
    "## Complexity Analysis\n"
    "- `search_graph(min_degree=10)` — high fan-out functions\n"
    "- `search_graph(label=\"Function\", sort_by=\"degree\")` — most-connected functions\n";

static const char skill_reference_content[] =
    "---\n"
    "name: codebase-memory-reference\n"
    "description: Codebase-memory-mcp reference guide. ALWAYS invoke this skill when the user "
    "asks about MCP tools, graph queries, Cypher syntax, edge types, or how to use the "
    "knowledge graph. Do not guess tool parameters — load this reference first.\n"
    "---\n"
    "\n"
    "# Codebase Memory MCP Reference\n"
    "\n"
    "## 14 total MCP Tools\n"
    "- `index_repository` — index a project\n"
    "- `index_status` — check indexing progress\n"
    "- `detect_changes` — find what changed since last index\n"
    "- `search_graph` — find nodes by pattern\n"
    "- `search_code` — text search in source\n"
    "- `query_graph` — Cypher query language\n"
    "- `trace_call_path` — call chain traversal\n"
    "- `get_code_snippet` — read function source\n"
    "- `get_graph_schema` — node/edge type catalog\n"
    "- `get_architecture` — high-level summary\n"
    "- `list_projects` — indexed projects\n"
    "- `delete_project` — remove a project\n"
    "- `manage_adr` — architecture decision records\n"
    "- `ingest_traces` — import runtime traces\n"
    "\n"
    "## Edge Types\n"
    "CALLS, HTTP_CALLS, ASYNC_CALLS, IMPORTS, DEFINES, DEFINES_METHOD,\n"
    "HANDLES, IMPLEMENTS, CONTAINS_FILE, CONTAINS_FOLDER, CONTAINS_PACKAGE\n"
    "\n"
    "## Cypher Examples\n"
    "```\n"
    "MATCH (f:Function) WHERE f.name =~ '.*Handler.*' RETURN f.name, f.file_path\n"
    "MATCH (a)-[r:CALLS]->(b) WHERE a.name = 'main' RETURN b.name\n"
    "MATCH (a)-[r:HTTP_CALLS]->(b) RETURN a.name, b.name, r.url_path\n"
    "```\n";

static const char codex_instructions_content[] =
    "# Codebase Knowledge Graph\n"
    "\n"
    "This project uses codebase-memory-mcp to maintain a knowledge graph of the codebase.\n"
    "Use the MCP tools to explore and understand the code:\n"
    "\n"
    "- `search_graph` — find functions, classes, routes by pattern\n"
    "- `trace_call_path` — trace who calls a function or what it calls\n"
    "- `get_code_snippet` — read function source code\n"
    "- `query_graph` — run Cypher queries for complex patterns\n"
    "- `get_architecture` — high-level project summary\n"
    "\n"
    "Always prefer graph tools over grep for code discovery.\n";

static const cbm_skill_t skills[CBM_SKILL_COUNT] = {
    {"codebase-memory-exploring", skill_exploring_content},
    {"codebase-memory-tracing", skill_tracing_content},
    {"codebase-memory-quality", skill_quality_content},
    {"codebase-memory-reference", skill_reference_content},
};

const cbm_skill_t *cbm_get_skills(void) {
    return skills;
}

const char *cbm_get_codex_instructions(void) {
    return codex_instructions_content;
}

/* ── Recursive mkdir (via compat_fs) ──────────────────────────── */

static int mkdirp(const char *path, int mode) {
    return (int)cbm_mkdir_p(path, mode) ? 0 : -1;
}

/* ── Recursive rmdir ──────────────────────────────────────────── */

// NOLINTNEXTLINE(misc-no-recursion) — intentional recursive directory removal
static int rmdir_recursive(const char *path) {
    cbm_dir_t *d = cbm_opendir(path);
    if (!d) {
        return -1;
    }

    cbm_dirent_t *ent;
    while ((ent = cbm_readdir(d)) != NULL) {
        char child[1024];
        snprintf(child, sizeof(child), "%s/%s", path, ent->name);
        struct stat st;
        if (stat(child, &st) == 0 && S_ISDIR(st.st_mode)) {
            rmdir_recursive(child);
        } else {
            cbm_unlink(child);
        }
    }
    cbm_closedir(d);
    return cbm_rmdir(path);
}

/* ── Skill management ─────────────────────────────────────────── */

int cbm_install_skills(const char *skills_dir, bool force, bool dry_run) {
    if (!skills_dir) {
        return 0;
    }
    int count = 0;

    for (int i = 0; i < CBM_SKILL_COUNT; i++) {
        char skill_path[1024];
        snprintf(skill_path, sizeof(skill_path), "%s/%s", skills_dir, skills[i].name);
        char file_path[1024];
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
        (void)fwrite(skills[i].content, 1, strlen(skills[i].content), f);
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
        char skill_path[1024];
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

    char old_path[1024];
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

    if (size <= 0 || size > 10L * 1024 * 1024) {
        (void)fclose(f);
        return NULL;
    }

    char *buf = malloc((size_t)size + 1);
    if (!buf) {
        (void)fclose(f);
        return NULL;
    }

    size_t nread = fread(buf, 1, (size_t)size, f);
    (void)fclose(f);
    // NOLINTNEXTLINE(clang-analyzer-security.ArrayBound)
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
    char dir[1024];
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
        return -1;
    }

    FILE *f = fopen(path, "w");
    if (!f) {
        free(json);
        return -1;
    }

    size_t written = fwrite(json, 1, len, f);
    /* Add trailing newline */
    (void)fputc('\n', f);
    (void)fclose(f);
    free(json);

    return written == len ? 0 : -1;
}

/* ── Editor MCP: Cursor/Windsurf/Gemini (mcpServers key) ──────── */

int cbm_install_editor_mcp(const char *binary_path, const char *config_path) {
    if (!binary_path || !config_path) {
        return -1;
    }

    /* Read existing or start fresh */
    yyjson_mut_doc *mdoc = yyjson_mut_doc_new(NULL);
    if (!mdoc) {
        return -1;
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
        return -1;
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
        return -1;
    }

    yyjson_doc *doc = read_json_file(config_path);
    if (!doc) {
        return -1;
    }

    yyjson_mut_doc *mdoc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_val_mut_copy(mdoc, yyjson_doc_get_root(doc));
    yyjson_doc_free(doc);
    if (!root) {
        yyjson_mut_doc_free(mdoc);
        return -1;
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
        return -1;
    }

    yyjson_mut_doc *mdoc = yyjson_mut_doc_new(NULL);
    if (!mdoc) {
        return -1;
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
        return -1;
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
        return -1;
    }

    yyjson_doc *doc = read_json_file(config_path);
    if (!doc) {
        return -1;
    }

    yyjson_mut_doc *mdoc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_val_mut_copy(mdoc, yyjson_doc_get_root(doc));
    yyjson_doc_free(doc);
    if (!root) {
        yyjson_mut_doc_free(mdoc);
        return -1;
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
        return -1;
    }

    yyjson_mut_doc *mdoc = yyjson_mut_doc_new(NULL);
    if (!mdoc) {
        return -1;
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
        return -1;
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
        return -1;
    }

    yyjson_doc *doc = read_json_file(config_path);
    if (!doc) {
        return -1;
    }

    yyjson_mut_doc *mdoc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_val_mut_copy(mdoc, yyjson_doc_get_root(doc));
    yyjson_doc_free(doc);
    if (!root) {
        yyjson_mut_doc_free(mdoc);
        return -1;
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

cbm_detected_agents_t cbm_detect_agents(const char *home_dir) {
    cbm_detected_agents_t agents;
    memset(&agents, 0, sizeof(agents));
    if (!home_dir || !home_dir[0]) {
        return agents;
    }

    char path[1024];
    struct stat st;

    /* Claude Code: ~/.claude/ */
    snprintf(path, sizeof(path), "%s/.claude", home_dir);
    if (stat(path, &st) == 0 && S_ISDIR(st.st_mode)) {
        agents.claude_code = true;
    }

    /* Codex CLI: ~/.codex/ */
    snprintf(path, sizeof(path), "%s/.codex", home_dir);
    if (stat(path, &st) == 0 && S_ISDIR(st.st_mode)) {
        agents.codex = true;
    }

    /* Gemini CLI: ~/.gemini/ */
    snprintf(path, sizeof(path), "%s/.gemini", home_dir);
    if (stat(path, &st) == 0 && S_ISDIR(st.st_mode)) {
        agents.gemini = true;
    }

    /* Zed: platform-specific config dir */
#ifdef __APPLE__
    snprintf(path, sizeof(path), "%s/Library/Application Support/Zed", home_dir);
#else
    snprintf(path, sizeof(path), "%s/.config/zed", home_dir);
#endif
    if (stat(path, &st) == 0 && S_ISDIR(st.st_mode)) {
        agents.zed = true;
    }

    /* OpenCode: binary on PATH */
    const char *oc = cbm_find_cli("opencode", home_dir);
    if (oc[0]) {
        agents.opencode = true;
    }

    /* Antigravity: ~/.gemini/antigravity/ */
    snprintf(path, sizeof(path), "%s/.gemini/antigravity", home_dir);
    if (stat(path, &st) == 0 && S_ISDIR(st.st_mode)) {
        agents.antigravity = true;
        agents.gemini = true; /* parent dir implies gemini */
    }

    /* Aider: binary on PATH */
    const char *ai = cbm_find_cli("aider", home_dir);
    if (ai[0]) {
        agents.aider = true;
    }

    /* KiloCode: globalStorage dir */
    snprintf(path, sizeof(path), "%s/.config/Code/User/globalStorage/kilocode.kilo-code", home_dir);
    if (stat(path, &st) == 0 && S_ISDIR(st.st_mode)) {
        agents.kilocode = true;
    }

    /* VS Code: User config dir */
#ifdef __APPLE__
    snprintf(path, sizeof(path), "%s/Library/Application Support/Code/User", home_dir);
#else
    snprintf(path, sizeof(path), "%s/.config/Code/User", home_dir);
#endif
    if (stat(path, &st) == 0 && S_ISDIR(st.st_mode)) {
        agents.vscode = true;
    }

    /* OpenClaw: ~/.openclaw/ dir */
    snprintf(path, sizeof(path), "%s/.openclaw", home_dir);
    if (stat(path, &st) == 0 && S_ISDIR(st.st_mode)) {
        agents.openclaw = true;
    }

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
    "2. `trace_call_path` — trace who calls a function or what it calls\n"
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
    "- Who calls it: `trace_call_path(function_name=\"OrderHandler\", direction=\"inbound\")`\n"
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
    if (size < 0 || size > 10L * 1024 * 1024) { /* cap at 10 MB */
        (void)fclose(f);
        return NULL;
    }

    char *buf = malloc((size_t)size + 1);
    if (!buf) {
        (void)fclose(f);
        return NULL;
    }
    size_t nread = fread(buf, 1, (size_t)size, f);
    (void)fclose(f);
    buf[nread] = '\0';
    if (out_len) {
        *out_len = nread;
    }
    return buf;
}

/* Write string to file, creating parent dirs if needed. */
static int write_file_str(const char *path, const char *content) {
    /* Ensure parent directory */
    char dir[1024];
    snprintf(dir, sizeof(dir), "%s", path);
    char *last_slash = strrchr(dir, '/');
    if (last_slash) {
        *last_slash = '\0';
        mkdirp(dir, DIR_PERMS);
    }

    FILE *f = fopen(path, "w");
    if (!f) {
        return -1;
    }
    size_t len = strlen(content);
    size_t written = fwrite(content, 1, len, f);
    (void)fclose(f);
    return written == len ? 0 : -1;
}

int cbm_upsert_instructions(const char *path, const char *content) {
    if (!path || !content) {
        return -1;
    }

    size_t existing_len = 0;
    char *existing = read_file_str(path, &existing_len);

    /* Build the marker-wrapped section */
    size_t section_len =
        strlen(CMM_MARKER_START) + 1 + strlen(content) + strlen(CMM_MARKER_END) + 1;
    char *section = malloc(section_len + 1);
    if (!section) {
        free(existing);
        return -1;
    }
    snprintf(section, section_len + 1, "%s\n%s%s\n", CMM_MARKER_START, content, CMM_MARKER_END);

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
        result = malloc(new_len + 1);
        if (!result) {
            free(existing);
            free(section);
            return -1;
        }
        memcpy(result, existing, prefix_len);
        memcpy(result + prefix_len, section, strlen(section));
        memcpy(result + prefix_len + strlen(section), end, suffix_len);
        result[new_len] = '\0';
    } else {
        /* Append section */
        size_t new_len = existing_len + 1 + strlen(section);
        result = malloc(new_len + 1);
        if (!result) {
            free(existing);
            free(section);
            return -1;
        }
        memcpy(result, existing, existing_len);
        result[existing_len] = '\n';
        memcpy(result + existing_len + 1, section, strlen(section));
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
        return -1;
    }

    size_t len = 0;
    char *content = read_file_str(path, &len);
    if (!content) {
        return 1;
    }

    char *start = strstr(content, CMM_MARKER_START);
    char *end = start ? strstr(start, CMM_MARKER_END) : NULL;

    if (!start || !end) {
        free(content);
        return 1; /* not found */
    }

    end += strlen(CMM_MARKER_END);
    if (*end == '\n') {
        end++;
    }

    /* Also remove a leading newline before the start marker if present */
    if (start > content && *(start - 1) == '\n') {
        start--;
    }

    size_t prefix_len = (size_t)(start - content);
    size_t suffix_len = strlen(end);
    size_t new_len = prefix_len + suffix_len;
    char *result = malloc(new_len + 1);
    if (!result) {
        free(content);
        return -1;
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
        return -1;
    }

    size_t len = 0;
    char *content = read_file_str(config_path, &len);

    /* Build our TOML section */
    char section[1024];
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
        size_t new_len = prefix_len + strlen(section) + 1 + suffix_len;
        char *result = malloc(new_len + 1);
        if (!result) {
            free(content);
            return -1;
        }
        memcpy(result, content, prefix_len);
        memcpy(result + prefix_len, section, strlen(section));
        result[prefix_len + strlen(section)] = '\n';
        memcpy(result + prefix_len + strlen(section) + 1, suffix, suffix_len);
        result[new_len] = '\0';

        int rc = write_file_str(config_path, result);
        free(content);
        free(result);
        return rc;
    }

    /* Append our section */
    size_t new_len = len + 1 + strlen(section);
    char *result = malloc(new_len + 1);
    if (!result) {
        free(content);
        return -1;
    }
    memcpy(result, content, len);
    result[len] = '\n';
    memcpy(result + len + 1, section, strlen(section));
    result[new_len] = '\0';

    int rc = write_file_str(config_path, result);
    free(content);
    free(result);
    return rc;
}

int cbm_remove_codex_mcp(const char *config_path) {
    if (!config_path) {
        return -1;
    }

    size_t len = 0;
    char *content = read_file_str(config_path, &len);
    if (!content) {
        return 1;
    }

    char *existing = strstr(content, CODEX_CMM_SECTION);
    if (!existing) {
        free(content);
        return 1;
    }

    char *section_end = existing + strlen(CODEX_CMM_SECTION);
    char *next_section = strstr(section_end, "\n[");
    if (next_section) {
        next_section++;
    }

    /* Remove leading newline if present */
    if (existing > content && *(existing - 1) == '\n') {
        existing--;
    }

    size_t prefix_len = (size_t)(existing - content);
    const char *suffix = next_section ? next_section : "";
    size_t suffix_len = strlen(suffix);
    size_t new_len = prefix_len + suffix_len;
    char *result = malloc(new_len + 1);
    if (!result) {
        free(content);
        return -1;
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
        return -1;
    }

    yyjson_mut_doc *mdoc = yyjson_mut_doc_new(NULL);
    if (!mdoc) {
        return -1;
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
        return -1;
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
    yyjson_mut_obj_add_str(mdoc, entry, "command", binary_path);
    yyjson_mut_obj_add_val(mdoc, mcp, "codebase-memory-mcp", entry);

    int rc = write_json_file(config_path, mdoc);
    yyjson_mut_doc_free(mdoc);
    return rc;
}

int cbm_remove_opencode_mcp(const char *config_path) {
    if (!config_path) {
        return -1;
    }

    yyjson_doc *doc = read_json_file(config_path);
    if (!doc) {
        return -1;
    }

    yyjson_mut_doc *mdoc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_val_mut_copy(mdoc, yyjson_doc_get_root(doc));
    yyjson_doc_free(doc);
    if (!root) {
        yyjson_mut_doc_free(mdoc);
        return -1;
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
#define CMM_HOOK_COMMAND                                                        \
    "echo 'Reminder: prefer codebase-memory-mcp search_graph/trace_call_path/"  \
    "get_code_snippet over Grep/Glob/Read/Search for code discovery. "          \
    "Use get_code_snippet(qualified_name) instead of Read to view a function. " \
    "Fall back only if MCP returns insufficient results.' >&2"

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
static int upsert_hooks_json(const char *settings_path, const char *hook_event,
                             const char *matcher_str, const char *command_str) {
    if (!settings_path) {
        return -1;
    }

    yyjson_mut_doc *mdoc = yyjson_mut_doc_new(NULL);
    if (!mdoc) {
        return -1;
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
        return -1;
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
static int remove_hooks_json(const char *settings_path, const char *hook_event,
                             const char *matcher_str) {
    if (!settings_path) {
        return -1;
    }

    yyjson_doc *doc = read_json_file(settings_path);
    if (!doc) {
        return -1;
    }

    yyjson_mut_doc *mdoc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_val_mut_copy(mdoc, yyjson_doc_get_root(doc));
    yyjson_doc_free(doc);
    if (!root) {
        yyjson_mut_doc_free(mdoc);
        return -1;
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
    return upsert_hooks_json(settings_path, "PreToolUse", CMM_HOOK_MATCHER, CMM_HOOK_COMMAND);
}

int cbm_remove_claude_hooks(const char *settings_path) {
    return remove_hooks_json(settings_path, "PreToolUse", CMM_HOOK_MATCHER);
}

#define GEMINI_HOOK_MATCHER "google_search|read_file|grep_search"
#define GEMINI_HOOK_COMMAND                                                    \
    "echo 'Reminder: prefer codebase-memory-mcp search_graph/trace_call_path/" \
    "get_code_snippet over grep/file search for code discovery.' >&2"

int cbm_upsert_gemini_hooks(const char *settings_path) {
    return upsert_hooks_json(settings_path, "BeforeTool", GEMINI_HOOK_MATCHER, GEMINI_HOOK_COMMAND);
}

int cbm_remove_gemini_hooks(const char *settings_path) {
    return remove_hooks_json(settings_path, "BeforeTool", GEMINI_HOOK_MATCHER);
}

/* ── PATH management ──────────────────────────────────────────── */

int cbm_ensure_path(const char *bin_dir, const char *rc_file, bool dry_run) {
    if (!bin_dir || !rc_file) {
        return -1;
    }

    char line[1024];
    snprintf(line, sizeof(line), "export PATH=\"%s:$PATH\"", bin_dir);

    /* Check if already present in rc file */
    FILE *f = fopen(rc_file, "r");
    if (f) {
        char buf[2048];
        while (fgets(buf, sizeof(buf), f)) {
            if (strstr(buf, line)) {
                (void)fclose(f);
                return 1; /* already present */
            }
        }
        (void)fclose(f);
    }

    if (dry_run) {
        return 0;
    }

    f = fopen(rc_file, "a");
    if (!f) {
        return -1;
    }

    // NOLINTNEXTLINE(clang-analyzer-security.insecureAPI.DeprecatedOrUnsafeBufferHandling)
    (void)fprintf(f, "\n# Added by codebase-memory-mcp install\n%s\n", line);
    (void)fclose(f);
    return 0;
}

/* ── Tar.gz extraction ────────────────────────────────────────── */

unsigned char *cbm_extract_binary_from_targz(const unsigned char *data, int data_len,
                                             int *out_len) {
    if (!data || data_len <= 0 || !out_len) {
        return NULL;
    }

    /* Decompress gzip */
    z_stream strm = {0};
    // NOLINTNEXTLINE(performance-no-int-to-ptr)
    strm.next_in = (unsigned char *)(uintptr_t)data;
    strm.avail_in = (unsigned int)data_len;

    // NOLINTNEXTLINE(misc-include-cleaner) — MAX_WBITS provided by standard header
    if (inflateInit2(&strm, 16 + MAX_WBITS) != Z_OK) {
        return NULL;
    }

    /* Allocate decompression buffer (up to 500MB) */
    size_t buf_cap = (size_t)data_len * 10;
    if (buf_cap < 4096) {
        buf_cap = 4096;
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
            size_t new_cap = buf_cap * 2;
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

    /* Parse tar: find entry starting with "codebase-memory-mcp" */
    size_t pos = 0;
    while (pos + 512 <= total) {
        /* Tar header is 512 bytes */
        const unsigned char *hdr = decompressed + pos;

        /* Check for end-of-archive (two zero blocks) */
        bool all_zero = true;
        for (int i = 0; i < 512 && all_zero; i++) {
            if (hdr[i] != 0) {
                all_zero = false;
            }
        }
        if (all_zero) {
            break;
        }

        /* Extract filename (bytes 0-99) */
        char name[TAR_NAME_LEN] = {0};
        memcpy(name, hdr, TAR_NAME_LEN - 1);

        /* Extract size from octal field */
        char size_str[TAR_SIZE_LEN] = {0};
        memcpy(size_str, hdr + TAR_SIZE_OFFSET, TAR_SIZE_LEN - 1);
        long file_size = strtol(size_str, NULL, 8);

        /* Extract type flag (byte 156) */
        char typeflag = (char)hdr[TAR_TYPE_OFFSET];

        pos += 512; /* skip header */

        /* Check if this is a regular file with our binary name */
        if (typeflag == '0' || typeflag == '\0') {
            /* Get basename */
            const char *basename = strrchr(name, '/');
            basename = basename ? basename + 1 : name;

            if (strncmp(basename, TAR_BINARY_NAME, TAR_BINARY_NAME_LEN) == 0) {
                if (pos + (size_t)file_size <= total) {
                    unsigned char *result = malloc((size_t)file_size);
                    if (result) {
                        memcpy(result, decompressed + pos, (size_t)file_size);
                        *out_len = (int)file_size;
                        free(decompressed);
                        return result;
                    }
                }
            }
        }

        /* Skip to next 512-byte boundary */
        size_t blocks = ((size_t)file_size + TAR_BLOCK_MASK) / TAR_BLOCK_SIZE;
        pos += blocks * 512;
    }

    free(decompressed);
    return NULL; /* binary not found */
}

/* ── Index management ─────────────────────────────────────────── */

static const char *get_cache_dir(const char *home_dir) {
    static char buf[1024];
    if (!home_dir) {
        home_dir = getenv("HOME");
    }
    if (!home_dir) {
        return NULL;
    }
    snprintf(buf, sizeof(buf), "%s/.cache/codebase-memory-mcp", home_dir);
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
        if (len > 3 && strcmp(ent->name + len - 3, ".db") == 0) {
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
        if (len > 3 && strcmp(ent->name + len - 3, ".db") == 0) {
            char path[1024];
            snprintf(path, sizeof(path), "%s/%s", cache_dir, ent->name);
            /* Also remove .db.tmp if present */
            char tmp_path[1040];
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
    char get_buf[4096]; /* static buffer for cbm_config_get return values */
};

cbm_config_t *cbm_config_open(const char *cache_dir) {
    if (!cache_dir) {
        return NULL;
    }

    char dbpath[1024];
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

    cbm_config_t *cfg = calloc(1, sizeof(*cfg));
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
    if (sqlite3_prepare_v2(cfg->db, "SELECT value FROM config WHERE key = ?", -1, &stmt, NULL) !=
        SQLITE_OK) {
        return default_val;
    }
    sqlite3_bind_text(stmt, 1, key, -1, SQLITE_TRANSIENT);

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
    long v = strtol(val, &endptr, 10);
    if (endptr == val || *endptr != '\0') {
        return default_val;
    }
    return (int)v;
}

int cbm_config_set(cbm_config_t *cfg, const char *key, const char *value) {
    if (!cfg || !key || !value) {
        return -1;
    }

    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(cfg->db, "INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)", -1,
                           &stmt, NULL) != SQLITE_OK) {
        return -1;
    }
    sqlite3_bind_text(stmt, 1, key, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 2, value, -1, SQLITE_TRANSIENT);

    int rc = sqlite3_step(stmt) == SQLITE_DONE ? 0 : -1;
    sqlite3_finalize(stmt);
    return rc;
}

int cbm_config_delete(cbm_config_t *cfg, const char *key) {
    if (!cfg || !key) {
        return -1;
    }

    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(cfg->db, "DELETE FROM config WHERE key = ?", -1, &stmt, NULL) !=
        SQLITE_OK) {
        return -1;
    }
    sqlite3_bind_text(stmt, 1, key, -1, SQLITE_TRANSIENT);

    int rc = sqlite3_step(stmt) == SQLITE_DONE ? 0 : -1;
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

    // NOLINTNEXTLINE(concurrency-mt-unsafe)
    const char *home = getenv("HOME");
    if (!home) {
        fprintf(stderr, "error: HOME not set\n");
        return 1;
    }

    char cache_dir[1024];
    snprintf(cache_dir, sizeof(cache_dir), "%s/.cache/codebase-memory-mcp", home);

    cbm_config_t *cfg = cbm_config_open(cache_dir);
    if (!cfg) {
        fprintf(stderr, "error: cannot open config database\n");
        return 1;
    }

    int rc = 0;
    if (strcmp(argv[0], "list") == 0 || strcmp(argv[0], "ls") == 0) {
        printf("Configuration:\n");
        printf("  %-25s = %-10s\n", CBM_CONFIG_AUTO_INDEX,
               cbm_config_get(cfg, CBM_CONFIG_AUTO_INDEX, "false"));
        printf("  %-25s = %-10s\n", CBM_CONFIG_AUTO_INDEX_LIMIT,
               cbm_config_get(cfg, CBM_CONFIG_AUTO_INDEX_LIMIT, "50000"));
    } else if (strcmp(argv[0], "get") == 0) {
        if (argc < 2) {
            fprintf(stderr, "Usage: config get <key>\n");
            rc = 1;
        } else {
            printf("%s\n", cbm_config_get(cfg, argv[1], ""));
        }
    } else if (strcmp(argv[0], "set") == 0) {
        if (argc < 3) {
            fprintf(stderr, "Usage: config set <key> <value>\n");
            rc = 1;
        } else {
            if (cbm_config_set(cfg, argv[1], argv[2]) == 0) {
                printf("%s = %s\n", argv[1], argv[2]);
            } else {
                fprintf(stderr, "error: failed to set %s\n", argv[1]);
                rc = 1;
            }
        }
    } else if (strcmp(argv[0], "reset") == 0) {
        if (argc < 2) {
            fprintf(stderr, "Usage: config reset <key>\n");
            rc = 1;
        } else {
            cbm_config_delete(cfg, argv[1]);
            printf("%s reset to default\n", argv[1]);
        }
    } else {
        fprintf(stderr, "Unknown config command: %s\n", argv[0]);
        rc = 1;
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
            g_auto_answer = 1;
        }
        if (strcmp(argv[i], "-n") == 0 || strcmp(argv[i], "--no") == 0) {
            g_auto_answer = -1;
        }
    }
}

static bool prompt_yn(const char *question) {
    if (g_auto_answer == 1) {
        printf("%s (y/n): y (auto)\n", question);
        return true;
    }
    if (g_auto_answer == -1) {
        printf("%s (y/n): n (auto)\n", question);
        return false;
    }

    printf("%s (y/n): ", question);
    (void)fflush(stdout);

    char buf[16];
    if (!fgets(buf, sizeof(buf), stdin)) {
        return false;
    }
    // NOLINTNEXTLINE(readability-implicit-bool-conversion)
    return (buf[0] == 'y' || buf[0] == 'Y') ? true : false;
}

/* ── SHA-256 checksum verification ─────────────────────────────── */

/* SHA-256 hex digest: 64 hex chars + NUL */
#define SHA256_HEX_LEN 64
#define SHA256_BUF_SIZE (SHA256_HEX_LEN + 1)
/* Minimum line length in checksums.txt: 64 hex + 2 spaces + 1 char filename */
#define CHECKSUM_LINE_MIN (SHA256_HEX_LEN + 2)

/* Compute SHA-256 of a file using platform tools (sha256sum/shasum).
 * Writes 64-char hex digest + NUL to out. Returns 0 on success. */
static int sha256_file(const char *path, char *out, size_t out_size) {
    if (out_size < SHA256_BUF_SIZE) {
        return -1;
    }
    char cmd[1024];
#ifdef __APPLE__
    snprintf(cmd, sizeof(cmd), "shasum -a 256 '%s' 2>/dev/null", path);
#else
    snprintf(cmd, sizeof(cmd), "sha256sum '%s' 2>/dev/null", path);
#endif
    // NOLINTNEXTLINE(bugprone-command-processor,cert-env33-c)
    FILE *fp = cbm_popen(cmd, "r");
    if (!fp) {
        return -1;
    }
    char line[256];
    if (fgets(line, sizeof(line), fp)) {
        /* Output format: <64-char hash>  <filename> */
        char *space = strchr(line, ' ');
        if (space && space - line == SHA256_HEX_LEN) {
            memcpy(out, line, SHA256_HEX_LEN);
            out[SHA256_HEX_LEN] = '\0';
            cbm_pclose(fp);
            return 0;
        }
    }
    cbm_pclose(fp);
    return -1;
}

/* Download checksums.txt and verify the archive integrity.
 * Returns: 0 = verified OK, 1 = mismatch (FAIL), -1 = could not verify (warning). */
static int verify_download_checksum(const char *archive_path, const char *archive_name) {
    char checksum_file[256];
    snprintf(checksum_file, sizeof(checksum_file), "%s/cbm-checksums.txt", cbm_tmpdir());

    char cmd[1024];
    snprintf(cmd, sizeof(cmd),
             "curl -fsSL -o '%s' "
             "'https://github.com/DeusData/codebase-memory-mcp/releases/latest/download/"
             "checksums.txt' 2>/dev/null",
             checksum_file);
    // NOLINTNEXTLINE(cert-env33-c) — intentional CLI subprocess for download
    int rc = system(cmd);
    if (rc != 0) {
        fprintf(stderr, "warning: could not download checksums.txt — skipping verification\n");
        cbm_unlink(checksum_file);
        return -1;
    }

    FILE *fp = fopen(checksum_file, "r");
    cbm_unlink(checksum_file);
    if (!fp) {
        return -1;
    }

    char expected[SHA256_BUF_SIZE] = {0};
    char line[512];
    while (fgets(line, sizeof(line), fp)) {
        /* Format: <64-char sha256>  <filename>\n */
        if (strlen(line) > CHECKSUM_LINE_MIN && strstr(line, archive_name)) {
            memcpy(expected, line, SHA256_HEX_LEN);
            expected[SHA256_HEX_LEN] = '\0';
            break;
        }
    }
    fclose(fp);

    if (expected[0] == '\0') {
        fprintf(stderr, "warning: %s not found in checksums.txt\n", archive_name);
        return -1;
    }

    char actual[SHA256_BUF_SIZE] = {0};
    if (sha256_file(archive_path, actual, sizeof(actual)) != 0) {
        fprintf(stderr, "warning: sha256sum/shasum not available — skipping verification\n");
        return -1;
    }

    if (strcmp(expected, actual) != 0) {
        fprintf(stderr, "error: CHECKSUM MISMATCH — downloaded binary may be compromised!\n");
        fprintf(stderr, "  expected: %s\n", expected);
        fprintf(stderr, "  actual:   %s\n", actual);
        return 1;
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

/* ── Subcommand: install ──────────────────────────────────────── */

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

    const char *home = getenv("HOME");
    if (!home) {
        fprintf(stderr, "error: HOME not set\n");
        return 1;
    }

    printf("codebase-memory-mcp install %s\n\n", CBM_VERSION);

    /* Step 1: Check for existing indexes */
    int index_count = 0;
    const char *cache_dir = get_cache_dir(home);
    if (cache_dir) {
        cbm_dir_t *d = cbm_opendir(cache_dir);
        if (d) {
            cbm_dirent_t *ent;
            while ((ent = cbm_readdir(d)) != NULL) {
                size_t len = strlen(ent->name);
                if (len > 3 && strcmp(ent->name + len - 3, ".db") == 0) {
                    index_count++;
                }
            }
            cbm_closedir(d);
        }
    }

    if (index_count > 0) {
        printf("Found %d existing index(es) that must be rebuilt:\n", index_count);
        cbm_list_indexes(home);
        printf("\n");
        if (!prompt_yn("Delete these indexes and continue with install?")) {
            printf("Install cancelled.\n");
            return 1;
        }
        if (!dry_run) {
            int removed = cbm_remove_indexes(home);
            printf("Removed %d index(es).\n\n", removed);
        }
    }

    /* Step 2: Binary path */
    char self_path[1024];
    snprintf(self_path, sizeof(self_path), "%s/.local/bin/codebase-memory-mcp", home);

    /* Step 3: Detect agents */
    cbm_detected_agents_t agents = cbm_detect_agents(home);
    printf("Detected agents:");
    if (agents.claude_code) {
        printf(" Claude-Code");
    }
    if (agents.codex) {
        printf(" Codex");
    }
    if (agents.gemini) {
        printf(" Gemini-CLI");
    }
    if (agents.zed) {
        printf(" Zed");
    }
    if (agents.opencode) {
        printf(" OpenCode");
    }
    if (agents.antigravity) {
        printf(" Antigravity");
    }
    if (agents.aider) {
        printf(" Aider");
    }
    if (agents.kilocode) {
        printf(" KiloCode");
    }
    if (agents.vscode) {
        printf(" VS-Code");
    }
    if (agents.openclaw) {
        printf(" OpenClaw");
    }
    if (!agents.claude_code && !agents.codex && !agents.gemini && !agents.zed && !agents.opencode &&
        !agents.antigravity && !agents.aider && !agents.kilocode && !agents.vscode &&
        !agents.openclaw) {
        printf(" (none)");
    }
    printf("\n\n");

    /* Step 4: Install Claude Code skills + hooks */
    if (agents.claude_code) {
        char skills_dir[1024];
        snprintf(skills_dir, sizeof(skills_dir), "%s/.claude/skills", home);
        printf("Claude Code:\n");

        int skill_count = cbm_install_skills(skills_dir, force, dry_run);
        printf("  skills: %d installed\n", skill_count);

        if (cbm_remove_old_monolithic_skill(skills_dir, dry_run)) {
            printf("  removed old monolithic skill\n");
        }

        /* MCP config — write to both locations for compatibility.
         * Claude Code <=2.1.x reads ~/.claude/.mcp.json
         * Claude Code >=2.1.80 reads ~/.claude.json */
        char mcp_path[1024];
        snprintf(mcp_path, sizeof(mcp_path), "%s/.claude/.mcp.json", home);
        if (!dry_run) {
            cbm_install_editor_mcp(self_path, mcp_path);
        }
        printf("  mcp: %s\n", mcp_path);

        char mcp_path2[1024];
        snprintf(mcp_path2, sizeof(mcp_path2), "%s/.claude.json", home);
        if (!dry_run) {
            cbm_install_editor_mcp(self_path, mcp_path2);
        }
        printf("  mcp: %s\n", mcp_path2);

        /* PreToolUse hook */
        char settings_path[1024];
        snprintf(settings_path, sizeof(settings_path), "%s/.claude/settings.json", home);
        if (!dry_run) {
            cbm_upsert_claude_hooks(settings_path);
        }
        printf("  hooks: PreToolUse (Grep|Glob reminder)\n");
    }

    /* Step 5: Install Codex CLI */
    if (agents.codex) {
        printf("Codex CLI:\n");
        char config_path[1024];
        snprintf(config_path, sizeof(config_path), "%s/.codex/config.toml", home);
        if (!dry_run) {
            cbm_upsert_codex_mcp(self_path, config_path);
        }
        printf("  mcp: %s\n", config_path);

        char instr_path[1024];
        snprintf(instr_path, sizeof(instr_path), "%s/.codex/AGENTS.md", home);
        if (!dry_run) {
            cbm_upsert_instructions(instr_path, agent_instructions_content);
        }
        printf("  instructions: %s\n", instr_path);
    }

    /* Step 6: Install Gemini CLI */
    if (agents.gemini) {
        printf("Gemini CLI:\n");
        char config_path[1024];
        snprintf(config_path, sizeof(config_path), "%s/.gemini/settings.json", home);
        if (!dry_run) {
            cbm_install_editor_mcp(self_path, config_path);
        }
        printf("  mcp: %s\n", config_path);

        char instr_path[1024];
        snprintf(instr_path, sizeof(instr_path), "%s/.gemini/GEMINI.md", home);
        if (!dry_run) {
            cbm_upsert_instructions(instr_path, agent_instructions_content);
        }
        printf("  instructions: %s\n", instr_path);

        /* BeforeTool hook (shared with Antigravity) */
        if (!dry_run) {
            cbm_upsert_gemini_hooks(config_path);
        }
        printf("  hooks: BeforeTool (grep/file search reminder)\n");
    }

    /* Step 7: Install Zed */
    if (agents.zed) {
        printf("Zed:\n");
        char config_path[1024];
#ifdef __APPLE__
        snprintf(config_path, sizeof(config_path),
                 "%s/Library/Application Support/Zed/settings.json", home);
#else
        snprintf(config_path, sizeof(config_path), "%s/.config/zed/settings.json", home);
#endif
        if (!dry_run) {
            cbm_install_zed_mcp(self_path, config_path);
        }
        printf("  mcp: %s\n", config_path);
    }

    /* Step 8: Install OpenCode */
    if (agents.opencode) {
        printf("OpenCode:\n");
        char config_path[1024];
        snprintf(config_path, sizeof(config_path), "%s/.config/opencode/opencode.json", home);
        if (!dry_run) {
            cbm_upsert_opencode_mcp(self_path, config_path);
        }
        printf("  mcp: %s\n", config_path);

        char instr_path[1024];
        snprintf(instr_path, sizeof(instr_path), "%s/.config/opencode/AGENTS.md", home);
        if (!dry_run) {
            cbm_upsert_instructions(instr_path, agent_instructions_content);
        }
        printf("  instructions: %s\n", instr_path);
    }

    /* Step 9: Install Antigravity */
    if (agents.antigravity) {
        printf("Antigravity:\n");
        char config_path[1024];
        snprintf(config_path, sizeof(config_path), "%s/.gemini/antigravity/mcp_config.json", home);
        if (!dry_run) {
            cbm_upsert_antigravity_mcp(self_path, config_path);
        }
        printf("  mcp: %s\n", config_path);

        char instr_path[1024];
        snprintf(instr_path, sizeof(instr_path), "%s/.gemini/antigravity/AGENTS.md", home);
        if (!dry_run) {
            cbm_upsert_instructions(instr_path, agent_instructions_content);
        }
        printf("  instructions: %s\n", instr_path);
    }

    /* Step 10: Install Aider */
    if (agents.aider) {
        printf("Aider:\n");
        char instr_path[1024];
        snprintf(instr_path, sizeof(instr_path), "%s/CONVENTIONS.md", home);
        if (!dry_run) {
            cbm_upsert_instructions(instr_path, agent_instructions_content);
        }
        printf("  instructions: %s\n", instr_path);
    }

    /* Step 11: Install KiloCode */
    if (agents.kilocode) {
        printf("KiloCode:\n");
        char config_path[1024];
        snprintf(config_path, sizeof(config_path),
                 "%s/.config/Code/User/globalStorage/kilocode.kilo-code/settings/mcp_settings.json",
                 home);
        if (!dry_run) {
            cbm_install_editor_mcp(self_path, config_path);
        }
        printf("  mcp: %s\n", config_path);

        /* KiloCode uses ~/.kilocode/rules/ for global instructions */
        char instr_path[1024];
        snprintf(instr_path, sizeof(instr_path), "%s/.kilocode/rules/codebase-memory-mcp.md", home);
        if (!dry_run) {
            cbm_upsert_instructions(instr_path, agent_instructions_content);
        }
        printf("  instructions: %s\n", instr_path);
    }

    /* Step 12: Install VS Code */
    if (agents.vscode) {
        printf("VS Code:\n");
        char config_path[1024];
#ifdef __APPLE__
        snprintf(config_path, sizeof(config_path),
                 "%s/Library/Application Support/Code/User/mcp.json", home);
#else
        snprintf(config_path, sizeof(config_path), "%s/.config/Code/User/mcp.json", home);
#endif
        if (!dry_run) {
            cbm_install_vscode_mcp(self_path, config_path);
        }
        printf("  mcp: %s\n", config_path);
    }

    /* Step 13: Install OpenClaw */
    if (agents.openclaw) {
        printf("OpenClaw:\n");
        char config_path[1024];
        snprintf(config_path, sizeof(config_path), "%s/.openclaw/openclaw.json", home);
        if (!dry_run) {
            cbm_install_editor_mcp(self_path, config_path);
        }
        printf("  mcp: %s\n", config_path);
    }

    /* Step 14: Ensure PATH */
    char bin_dir[1024];
    snprintf(bin_dir, sizeof(bin_dir), "%s/.local/bin", home);
    const char *rc = cbm_detect_shell_rc(home);
    if (rc[0]) {
        int path_rc = cbm_ensure_path(bin_dir, rc, dry_run);
        if (path_rc == 0) {
            printf("\nAdded %s to PATH in %s\n", bin_dir, rc);
        } else if (path_rc == 1) {
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

int cbm_cmd_uninstall(int argc, char **argv) {
    parse_auto_answer(argc, argv);
    bool dry_run = false;
    for (int i = 0; i < argc; i++) {
        if (strcmp(argv[i], "--dry-run") == 0) {
            dry_run = true;
        }
    }

    const char *home = getenv("HOME");
    if (!home) {
        fprintf(stderr, "error: HOME not set\n");
        return 1;
    }

    printf("codebase-memory-mcp uninstall\n\n");

    /* Step 1: Detect agents and remove per-agent configs */
    cbm_detected_agents_t agents = cbm_detect_agents(home);

    if (agents.claude_code) {
        char skills_dir[1024];
        snprintf(skills_dir, sizeof(skills_dir), "%s/.claude/skills", home);
        int removed = cbm_remove_skills(skills_dir, dry_run);
        printf("Claude Code: removed %d skill(s)\n", removed);

        char mcp_path[1024];
        snprintf(mcp_path, sizeof(mcp_path), "%s/.claude/.mcp.json", home);
        if (!dry_run) {
            cbm_remove_editor_mcp(mcp_path);
        }
        printf("  removed MCP config entry\n");

        /* Also remove from new location (Claude Code >=2.1.80) */
        char mcp_path2[1024];
        snprintf(mcp_path2, sizeof(mcp_path2), "%s/.claude.json", home);
        if (!dry_run) {
            cbm_remove_editor_mcp(mcp_path2);
        }

        char settings_path[1024];
        snprintf(settings_path, sizeof(settings_path), "%s/.claude/settings.json", home);
        if (!dry_run) {
            cbm_remove_claude_hooks(settings_path);
        }
        printf("  removed PreToolUse hook\n");
    }

    if (agents.codex) {
        char config_path[1024];
        snprintf(config_path, sizeof(config_path), "%s/.codex/config.toml", home);
        if (!dry_run) {
            cbm_remove_codex_mcp(config_path);
        }
        printf("Codex CLI: removed MCP config entry\n");

        char instr_path[1024];
        snprintf(instr_path, sizeof(instr_path), "%s/.codex/AGENTS.md", home);
        if (!dry_run) {
            cbm_remove_instructions(instr_path);
        }
        printf("  removed instructions\n");
    }

    if (agents.gemini) {
        char config_path[1024];
        snprintf(config_path, sizeof(config_path), "%s/.gemini/settings.json", home);
        if (!dry_run) {
            cbm_remove_editor_mcp(config_path);
        }
        printf("Gemini CLI: removed MCP config entry\n");

        if (!dry_run) {
            cbm_remove_gemini_hooks(config_path);
        }
        printf("  removed BeforeTool hook\n");

        char instr_path[1024];
        snprintf(instr_path, sizeof(instr_path), "%s/.gemini/GEMINI.md", home);
        if (!dry_run) {
            cbm_remove_instructions(instr_path);
        }
        printf("  removed instructions\n");
    }

    if (agents.zed) {
        char config_path[1024];
#ifdef __APPLE__
        snprintf(config_path, sizeof(config_path),
                 "%s/Library/Application Support/Zed/settings.json", home);
#else
        snprintf(config_path, sizeof(config_path), "%s/.config/zed/settings.json", home);
#endif
        if (!dry_run) {
            cbm_remove_zed_mcp(config_path);
        }
        printf("Zed: removed MCP config entry\n");
    }

    if (agents.opencode) {
        char config_path[1024];
        snprintf(config_path, sizeof(config_path), "%s/.config/opencode/opencode.json", home);
        if (!dry_run) {
            cbm_remove_opencode_mcp(config_path);
        }
        printf("OpenCode: removed MCP config entry\n");

        char instr_path[1024];
        snprintf(instr_path, sizeof(instr_path), "%s/.config/opencode/AGENTS.md", home);
        if (!dry_run) {
            cbm_remove_instructions(instr_path);
        }
        printf("  removed instructions\n");
    }

    if (agents.antigravity) {
        char config_path[1024];
        snprintf(config_path, sizeof(config_path), "%s/.gemini/antigravity/mcp_config.json", home);
        if (!dry_run) {
            cbm_remove_antigravity_mcp(config_path);
        }
        printf("Antigravity: removed MCP config entry\n");

        char instr_path[1024];
        snprintf(instr_path, sizeof(instr_path), "%s/.gemini/antigravity/AGENTS.md", home);
        if (!dry_run) {
            cbm_remove_instructions(instr_path);
        }
        printf("  removed instructions\n");
    }

    if (agents.aider) {
        char instr_path[1024];
        snprintf(instr_path, sizeof(instr_path), "%s/CONVENTIONS.md", home);
        if (!dry_run) {
            cbm_remove_instructions(instr_path);
        }
        printf("Aider: removed instructions\n");
    }

    if (agents.kilocode) {
        char config_path[1024];
        snprintf(config_path, sizeof(config_path),
                 "%s/.config/Code/User/globalStorage/kilocode.kilo-code/settings/mcp_settings.json",
                 home);
        if (!dry_run) {
            cbm_remove_editor_mcp(config_path);
        }
        printf("KiloCode: removed MCP config entry\n");

        char instr_path[1024];
        snprintf(instr_path, sizeof(instr_path), "%s/.kilocode/rules/codebase-memory-mcp.md", home);
        if (!dry_run) {
            cbm_remove_instructions(instr_path);
        }
        printf("  removed instructions\n");
    }

    if (agents.vscode) {
        char config_path[1024];
#ifdef __APPLE__
        snprintf(config_path, sizeof(config_path),
                 "%s/Library/Application Support/Code/User/mcp.json", home);
#else
        snprintf(config_path, sizeof(config_path), "%s/.config/Code/User/mcp.json", home);
#endif
        if (!dry_run) {
            cbm_remove_vscode_mcp(config_path);
        }
        printf("VS Code: removed MCP config entry\n");
    }

    if (agents.openclaw) {
        char config_path[1024];
        snprintf(config_path, sizeof(config_path), "%s/.openclaw/openclaw.json", home);
        if (!dry_run) {
            cbm_remove_editor_mcp(config_path);
        }
        printf("OpenClaw: removed MCP config entry\n");
    }

    /* Step 2: Remove indexes */
    int index_count = 0;
    const char *cache_dir = get_cache_dir(home);
    if (cache_dir) {
        cbm_dir_t *d = cbm_opendir(cache_dir);
        if (d) {
            cbm_dirent_t *ent;
            while ((ent = cbm_readdir(d)) != NULL) {
                size_t len = strlen(ent->name);
                if (len > 3 && strcmp(ent->name + len - 3, ".db") == 0) {
                    index_count++;
                }
            }
            cbm_closedir(d);
        }
    }

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
    char bin_path[1024];
    snprintf(bin_path, sizeof(bin_path), "%s/.local/bin/codebase-memory-mcp", home);
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

int cbm_cmd_update(int argc, char **argv) {
    parse_auto_answer(argc, argv);

    const char *home = getenv("HOME");
    if (!home) {
        fprintf(stderr, "error: HOME not set\n");
        return 1;
    }

    printf("codebase-memory-mcp update (current: %s)\n\n", CBM_VERSION);

    /* Step 1: Check for existing indexes */
    int index_count = 0;
    const char *cache_dir = get_cache_dir(home);
    if (cache_dir) {
        cbm_dir_t *d = cbm_opendir(cache_dir);
        if (d) {
            cbm_dirent_t *ent;
            while ((ent = cbm_readdir(d)) != NULL) {
                size_t len = strlen(ent->name);
                if (len > 3 && strcmp(ent->name + len - 3, ".db") == 0) {
                    index_count++;
                }
            }
            cbm_closedir(d);
        }
    }

    if (index_count > 0) {
        printf("Found %d existing index(es) that must be rebuilt after update:\n", index_count);
        cbm_list_indexes(home);
        printf("\n");
        if (!prompt_yn("Delete these indexes and continue with update?")) {
            printf("Update cancelled.\n");
            return 1;
        }
        int removed = cbm_remove_indexes(home);
        printf("Removed %d index(es).\n\n", removed);
    }

    /* Step 2: Ask for UI variant */
    printf("Which binary variant do you want?\n");
    printf("  1) standard  — MCP server only\n");
    printf("  2) ui        — MCP server + embedded graph visualization\n");
    printf("Choose (1/2): ");
    (void)fflush(stdout);

    char choice[16];
    if (!fgets(choice, sizeof(choice), stdin)) {
        fprintf(stderr, "error: failed to read input\n");
        return 1;
    }
    // NOLINTNEXTLINE(readability-implicit-bool-conversion)
    bool want_ui = (choice[0] == '2') ? true : false;
    // NOLINTNEXTLINE(readability-implicit-bool-conversion)
    const char *variant = want_ui ? "ui-" : "";
    // NOLINTNEXTLINE(readability-implicit-bool-conversion)
    const char *variant_label = want_ui ? "ui" : "standard";

    /* Step 3: Build download URL */
    const char *os = detect_os();
    const char *arch = detect_arch();
    const char *ext = strcmp(os, "windows") == 0 ? "zip" : "tar.gz";

    char url[512];
    if (want_ui) {
        snprintf(url, sizeof(url),
                 "https://github.com/DeusData/codebase-memory-mcp/releases/latest/download/"
                 "codebase-memory-mcp-ui-%s-%s.%s",
                 os, arch, ext);
    } else {
        snprintf(url, sizeof(url),
                 "https://github.com/DeusData/codebase-memory-mcp/releases/latest/download/"
                 "codebase-memory-mcp-%s-%s.%s",
                 os, arch, ext);
    }

    printf("\nDownloading %s binary for %s/%s ...\n", variant_label, os, arch);
    printf("  %s\n", url);

    /* Step 4: Download using curl */
    char tmp_archive[256];
    snprintf(tmp_archive, sizeof(tmp_archive), "%s/cbm-update.%s", cbm_tmpdir(), ext);

    char cmd[1024];
    snprintf(cmd, sizeof(cmd), "curl -fSL --progress-bar -o '%s' '%s'", tmp_archive, url);
    // NOLINTNEXTLINE(cert-env33-c) — intentional CLI subprocess for download
    int rc = system(cmd);
    if (rc != 0) {
        fprintf(stderr, "error: download failed (exit %d)\n", rc);
        cbm_unlink(tmp_archive);
        return 1;
    }

    /* Step 4b: Verify checksum */
    {
        /* Build the expected archive filename (matches checksums.txt format) */
        char archive_name[256];
        if (want_ui) {
            snprintf(archive_name, sizeof(archive_name), "codebase-memory-mcp-ui-%s-%s.%s", os,
                     arch, ext);
        } else {
            snprintf(archive_name, sizeof(archive_name), "codebase-memory-mcp-%s-%s.%s", os, arch,
                     ext);
        }
        int crc = verify_download_checksum(tmp_archive, archive_name);
        if (crc == 1) {
            /* Hard fail: checksum mismatch */
            cbm_unlink(tmp_archive);
            return 1;
        }
        /* crc == -1: could not verify (warning only), crc == 0: verified OK */
    }

    /* Step 5: Extract binary */
    char bin_dest[1024];
    snprintf(bin_dest, sizeof(bin_dest), "%s/.local/bin/codebase-memory-mcp", home);

    /* Ensure install directory exists */
    char bin_dir[1024];
    snprintf(bin_dir, sizeof(bin_dir), "%s/.local/bin", home);
    cbm_mkdir_p(bin_dir, 0755);

    if (strcmp(ext, "tar.gz") == 0) {
        /* Read archive into memory and extract */
        FILE *f = fopen(tmp_archive, "rb");
        if (!f) {
            fprintf(stderr, "error: cannot open %s\n", tmp_archive);
            return 1;
        }
        fseek(f, 0, SEEK_END);
        long fsize = ftell(f);
        fseek(f, 0, SEEK_SET);

        unsigned char *data = malloc((size_t)fsize);
        if (!data) {
            fclose(f);
            cbm_unlink(tmp_archive);
            return 1;
        }
        fread(data, 1, (size_t)fsize, f);
        fclose(f);

        int bin_len = 0;
        unsigned char *bin_data = cbm_extract_binary_from_targz(data, (int)fsize, &bin_len);
        free(data);
        cbm_unlink(tmp_archive);

        if (!bin_data || bin_len <= 0) {
            fprintf(stderr, "error: binary not found in archive\n");
            free(bin_data);
            return 1;
        }

        /* Open with final permissions atomically (no TOCTOU between write and chmod) */
#ifndef _WIN32
        int fd = open(bin_dest, O_WRONLY | O_CREAT | O_TRUNC, 0755);
        if (fd < 0) {
            fprintf(stderr, "error: cannot write to %s\n", bin_dest);
            free(bin_data);
            return 1;
        }
        FILE *out = fdopen(fd, "wb");
#else
        FILE *out = fopen(bin_dest, "wb");
#endif
        if (!out) {
            fprintf(stderr, "error: cannot write to %s\n", bin_dest);
            free(bin_data);
#ifndef _WIN32
            close(fd);
#endif
            return 1;
        }
        fwrite(bin_data, 1, (size_t)bin_len, out);
        fclose(out);
        free(bin_data);
    } else {
        /* Zip extraction: exec unzip directly without shell interpretation */
        const char *unzip_argv[] = {"unzip", "-o", "-d", bin_dir, tmp_archive, NULL};
        rc = cbm_exec_no_shell(unzip_argv);
        cbm_unlink(tmp_archive);
        if (rc != 0) {
            fprintf(stderr, "error: extraction failed\n");
            return 1;
        }
        /* Rename variant binary if needed */
        if (want_ui) {
            char ui_bin[1024];
            snprintf(ui_bin, sizeof(ui_bin), "%s/codebase-memory-mcp-ui.exe", bin_dir);
            snprintf(bin_dest, sizeof(bin_dest), "%s/codebase-memory-mcp.exe", bin_dir);
            rename(ui_bin, bin_dest);
        }
    }

    /* Step 6: Reinstall skills (force to pick up new content) */
    char skills_dir[1024];
    snprintf(skills_dir, sizeof(skills_dir), "%s/.claude/skills", home);
    int skill_count = cbm_install_skills(skills_dir, true, false);
    printf("Updated %d skill(s).\n", skill_count);

    /* Step 7: Verify new version (exec directly, no shell interpretation) */
    printf("\nUpdate complete. Verifying:\n");
    {
        const char *ver_argv[] = {bin_dest, "--version", NULL};
        (void)cbm_exec_no_shell(ver_argv);
    }

    printf("\nAll project indexes were cleared. They will be rebuilt\n");
    printf("automatically when you next use the MCP server.\n");
    (void)variant;
    return 0;
}
