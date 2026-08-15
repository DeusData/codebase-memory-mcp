/*
 * pass_envscan.c — Environment URL scanner.
 *
 * Walks a project directory, scans config files (Dockerfile, .env, shell,
 * YAML, TOML, Terraform, .properties) for environment variable assignments
 * where the value is a URL. Filters out secrets.
 *
 * Port of internal/pipeline/envscan.go:ScanProjectEnvURLs().
 */
#include "foundation/constants.h"

enum {
    ENV_REGEX_MAX = 5,
    ENV_GRP_1 = 1,
    ENV_GRP_2 = 2,
    ENV_GRP_3 = 3,
    ENV_GRP_4 = 4,
    ENV_GRP_5 = 5,
    ENV_EXT_LEN = 4, /* strlen(".env") */
};

#define SLEN(s) (sizeof(s) - 1)
#include "pipeline/pipeline.h"
#include "pipeline/pipeline_internal.h"
#include "foundation/log.h"

#include <ctype.h>
#include "foundation/compat_fs.h"
#include "foundation/compat_regex.h"
#include "foundation/platform.h" /* cbm_normalize_path_sep */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

#ifdef _WIN32
#include "foundation/win_utf8.h" /* cbm_path_to_wide */
#include <windows.h>
/* Canonical paths on both sides come from the same resolver, so casing already
 * agrees; comparing case-insensitively removes the doubt at no cost. */
#define envscan_path_ncmp _strnicmp
#else
#include <fcntl.h>
#include <unistd.h>
#define envscan_path_ncmp strncmp
#endif

/* ── Regex patterns (compiled lazily) ──────────────────────────── */

static cbm_regex_t dockerfile_re;  /* ENV|ARG KEY=VALUE or KEY VALUE */
static cbm_regex_t yaml_kv_re;     /* key: "https://..." */
static cbm_regex_t yaml_setenv_re; /* --set-env-vars KEY=VALUE */
static cbm_regex_t terraform_re;   /* default|value = "https://..." */
static cbm_regex_t shell_re;       /* [export] KEY=https://... */
static cbm_regex_t envfile_re;     /* KEY=https://... */
static cbm_regex_t toml_re;        /* key = "https://..." */
static cbm_regex_t properties_re;  /* key=https://... */
static int patterns_compiled = 0;

/* POSIX ERE doesn't support \w or \S — use bracket expressions */
#define W "[A-Za-z0-9_]" /* word char */
#define NW "[^ \t\"']"   /* non-whitespace, non-quote */

static void compile_patterns(void) {
    if (patterns_compiled) {
        return;
    }

    cbm_regcomp(&dockerfile_re, "^(ENV|ARG)[[:space:]]+(" W "+)[= ](.*)", CBM_REG_EXTENDED);
    cbm_regcomp(&yaml_kv_re, "(" W "+):[[:space:]]*[\"']?(https?://" NW "+)", CBM_REG_EXTENDED);
    cbm_regcomp(&yaml_setenv_re, "--set-env-vars[[:space:]]+(" W "+)=([^ \t]+)", CBM_REG_EXTENDED);
    cbm_regcomp(&terraform_re, "(default|value)[[:space:]]*=[[:space:]]*\"(https?://[^\"]+)\"",
                CBM_REG_EXTENDED);
    cbm_regcomp(&shell_re, "(export[[:space:]]+)?(" W "+)=[\"']?(https?://" NW "+)",
                CBM_REG_EXTENDED);
    cbm_regcomp(&envfile_re, "^(" W "+)=(https?://[^ \t]+)", CBM_REG_EXTENDED);
    cbm_regcomp(&toml_re, "(" W "+)[[:space:]]*=[[:space:]]*\"(https?://[^\"]+)\"",
                CBM_REG_EXTENDED);
    cbm_regcomp(&properties_re, "(" W "+)[[:space:]]*=[[:space:]]*(https?://[^ \t]+)",
                CBM_REG_EXTENDED);

    patterns_compiled = SKIP_ONE;
}

#undef W
#undef NW

/* ── File type detection ───────────────────────────────────────── */

static int is_dockerfile_name(const char *name) {
    /* Case-insensitive check */
    char lower[CBM_SZ_256];
    size_t len = strlen(name);
    if (len >= sizeof(lower)) {
        return 0;
    }
    for (size_t i = 0; i <= len; i++) {
        lower[i] = (char)tolower((unsigned char)name[i]);
    }

    if (strcmp(lower, "dockerfile") == 0) {
        return SKIP_ONE;
    }
#define DOCKERFILE_SUFFIX_LEN 11 /* strlen("dockerfile.") == strlen(".dockerfile") */
    if (strncmp(lower, "dockerfile.", DOCKERFILE_SUFFIX_LEN) == 0) {
        return SKIP_ONE;
    }
    if (len > DOCKERFILE_SUFFIX_LEN &&
        strcmp(lower + len - DOCKERFILE_SUFFIX_LEN, ".dockerfile") == 0) {
        return SKIP_ONE;
    }
    return 0;
}

static int is_env_file_name(const char *name) {
    char lower[CBM_SZ_256];
    size_t len = strlen(name);
    if (len >= sizeof(lower)) {
        return 0;
    }
    for (size_t i = 0; i <= len; i++) {
        lower[i] = (char)tolower((unsigned char)name[i]);
    }

    if (strcmp(lower, ".env") == 0) {
        return SKIP_ONE;
    }
    if (strncmp(lower, ".env.", SLEN(".env.")) == 0) {
        return SKIP_ONE;
    }
    if (len > ENV_EXT_LEN && strcmp(lower + len - ENV_EXT_LEN, ".env") == 0) {
        return SKIP_ONE;
    }
    return 0;
}

static int is_secret_file(const char *name) {
    char lower[CBM_SZ_256];
    size_t len = strlen(name);
    if (len >= sizeof(lower)) {
        return 0;
    }
    for (size_t i = 0; i <= len; i++) {
        lower[i] = (char)tolower((unsigned char)name[i]);
    }

    static const char *patterns[] = {
        "service_account", "credentials", "key.json", "key.pem", "id_rsa",
        "id_ed25519",      ".pem",        ".key",     NULL};
    for (int i = 0; patterns[i]; i++) {
        if (strstr(lower, patterns[i])) {
            return SKIP_ONE;
        }
    }
    return 0;
}

/* ── Ignored directories ───────────────────────────────────────── */

static int is_ignored_dir(const char *name) {
    static const char *dirs[] = {
        ".git",  "node_modules", ".svn", ".hg",   "__pycache__", "vendor", ".terraform", ".cache",
        ".idea", ".vscode",      "dist", "build", ".next",       ".nuxt",  "target",     NULL};
    for (int i = 0; dirs[i]; i++) {
        if (strcmp(name, dirs[i]) == 0) {
            return SKIP_ONE;
        }
    }
    return 0;
}

/* ── File type enum ────────────────────────────────────────────── */

typedef enum {
    FT_UNKNOWN = 0,
    FT_DOCKERFILE,
    FT_YAML,
    FT_TERRAFORM,
    FT_SHELL,
    FT_ENVFILE,
    FT_TOML,
    FT_PROPERTIES,
} file_type_t;

static file_type_t detect_file_type(const char *name) {
    if (is_dockerfile_name(name)) {
        return FT_DOCKERFILE;
    }
    if (is_env_file_name(name)) {
        return FT_ENVFILE;
    }

    const char *ext = strrchr(name, '.');
    if (!ext) {
        return FT_UNKNOWN;
    }

    if (strcmp(ext, ".yaml") == 0 || strcmp(ext, ".yml") == 0) {
        return FT_YAML;
    }
    if (strcmp(ext, ".tf") == 0 || strcmp(ext, ".hcl") == 0) {
        return FT_TERRAFORM;
    }
    if (strcmp(ext, ".sh") == 0 || strcmp(ext, ".bash") == 0 || strcmp(ext, ".zsh") == 0) {
        return FT_SHELL;
    }
    if (strcmp(ext, ".toml") == 0) {
        return FT_TOML;
    }
    if (strcmp(ext, ".properties") == 0 || strcmp(ext, ".cfg") == 0 || strcmp(ext, ".ini") == 0) {
        return FT_PROPERTIES;
    }

    return FT_UNKNOWN;
}

/* ── Line scanner ──────────────────────────────────────────────── */

/* Extract key/value from a regex match with two capture groups.
 * Returns 1 on success, 0 if groups are empty or too large. */
static int extract_kv_groups(const char *trimmed, const cbm_regmatch_t *m, int key_grp, int val_grp,
                             char *key_out, size_t key_sz, char *val_out, size_t val_sz) {
    int klen = (m[key_grp].rm_eo - m[key_grp].rm_so);
    int vlen = (m[val_grp].rm_eo - m[val_grp].rm_so);
    if (klen <= 0 || klen >= (int)key_sz || vlen <= 0 || vlen >= (int)val_sz) {
        return 0;
    }
    memcpy(key_out, trimmed + m[key_grp].rm_so, klen);
    key_out[klen] = '\0';
    memcpy(val_out, trimmed + m[val_grp].rm_so, vlen);
    val_out[vlen] = '\0';
    return SKIP_ONE;
}

/* Try to scan a Dockerfile line. */
static int scan_dockerfile_line(const char *line, char *key, size_t ksz, char *val, size_t vsz) {
    cbm_regmatch_t m[ENV_REGEX_MAX];
    if (cbm_regexec(&dockerfile_re, line, ENV_GRP_4, m, 0) != 0) {
        return 0;
    }
    if (!extract_kv_groups(line, m, ENV_GRP_2, ENV_GRP_3, key, ksz, val, vsz)) {
        return 0;
    }
    size_t vl = strlen(val);
    while (vl > 0 && (val[vl - SKIP_ONE] == '"' || val[vl - SKIP_ONE] == '\'')) {
        val[--vl] = '\0';
    }
    return SKIP_ONE;
}

/* Try to scan a YAML line. */
static int scan_yaml_line(const char *line, char *key, size_t ksz, char *val, size_t vsz) {
    cbm_regmatch_t m[ENV_REGEX_MAX];
    if (cbm_regexec(&yaml_kv_re, line, ENV_GRP_3, m, 0) == 0 &&
        extract_kv_groups(line, m, ENV_GRP_1, ENV_GRP_2, key, ksz, val, vsz)) {
        return SKIP_ONE;
    }
    if (cbm_regexec(&yaml_setenv_re, line, ENV_GRP_3, m, 0) == 0 &&
        extract_kv_groups(line, m, ENV_GRP_1, ENV_GRP_2, key, ksz, val, vsz)) {
        return SKIP_ONE;
    }
    return 0;
}

/* Try to scan a Terraform line. */
static int scan_terraform_line(const char *line, char *key, size_t ksz, char *val, size_t vsz) {
    cbm_regmatch_t m[ENV_REGEX_MAX];
    if (cbm_regexec(&terraform_re, line, ENV_GRP_3, m, 0) != 0) {
        return 0;
    }
    int vlen = (m[ENV_GRP_2].rm_eo - m[ENV_GRP_2].rm_so);
    if (vlen <= 0 || vlen >= (int)vsz) {
        return 0;
    }
    strncpy(key, "_tf_default", ksz - SKIP_ONE);
    key[ksz - SKIP_ONE] = '\0';
    memcpy(val, line + m[ENV_GRP_2].rm_so, vlen);
    val[vlen] = '\0';
    return SKIP_ONE;
}

/* Try single-regex scan (shell, envfile, toml, properties). */
static int scan_regex_line(cbm_regex_t *re, const char *line, int kg, int vg, char *key, size_t ksz,
                           char *val, size_t vsz) {
    cbm_regmatch_t m[ENV_REGEX_MAX];
    if (cbm_regexec(re, line, ENV_GRP_5, m, 0) == 0 &&
        extract_kv_groups(line, m, kg, vg, key, ksz, val, vsz)) {
        return SKIP_ONE;
    }
    return 0;
}

static int scan_line(const char *line, file_type_t ft, char *key_out, size_t key_sz, char *val_out,
                     size_t val_sz) {
    const char *trimmed = line;
    while (*trimmed == ' ' || *trimmed == '\t') {
        trimmed++;
    }
    if (*trimmed == '#' || (trimmed[0] == '/' && trimmed[SKIP_ONE] == '/')) {
        return 0;
    }

    switch (ft) {
    case FT_DOCKERFILE:
        return scan_dockerfile_line(trimmed, key_out, key_sz, val_out, val_sz);
    case FT_YAML:
        return scan_yaml_line(trimmed, key_out, key_sz, val_out, val_sz);
    case FT_TERRAFORM:
        return scan_terraform_line(trimmed, key_out, key_sz, val_out, val_sz);
    case FT_SHELL:
        return scan_regex_line(&shell_re, trimmed, ENV_GRP_2, ENV_GRP_3, key_out, key_sz, val_out,
                               val_sz);
    case FT_ENVFILE:
        return scan_regex_line(&envfile_re, trimmed, ENV_GRP_1, ENV_GRP_2, key_out, key_sz, val_out,
                               val_sz);
    case FT_TOML:
        return scan_regex_line(&toml_re, trimmed, ENV_GRP_1, ENV_GRP_2, key_out, key_sz, val_out,
                               val_sz);
    case FT_PROPERTIES:
        return scan_regex_line(&properties_re, trimmed, ENV_GRP_1, ENV_GRP_2, key_out, key_sz,
                               val_out, val_sz);
    default:
        return 0;
    }
}

/* ── Containment ───────────────────────────────────────────────── */

/* Join dir + name, rejecting rather than truncating.
 *
 * Silent truncation here was not merely a lost path. The root-relative suffix
 * used to be derived as `full_path + strlen(root_path)`, so once the real root
 * was longer than this fixed buffer that pointer landed past the end of the
 * array — out-of-bounds arithmetic on every entry below it. Refusing the path is
 * what keeps envscan_relative_suffix in bounds by construction. */
static bool envscan_join_path(char *out, size_t out_sz, const char *dir, const char *name) {
    if (out_sz == 0) {
        return false;
    }
    int written = snprintf(out, out_sz, "%s/%s", dir, name);
    if (written <= 0 || (size_t)written >= out_sz) {
        out[0] = '\0';
        cbm_log_warn("envscan.path_too_long", "dir", dir, "name", name);
        return false;
    }
    return true;
}

/* Root-relative suffix of a path built beneath root_path.
 *
 * The prefix comparison is not a formality: it succeeding is precisely what
 * proves `path` holds at least root_len bytes, which is what makes
 * `path + root_len` a valid pointer. Never compute this suffix from an
 * unverified strlen. Returns NULL when `path` is not under the root. */
static const char *envscan_relative_suffix(const char *path, const char *root_path,
                                           size_t root_len) {
    if (strncmp(path, root_path, root_len) != 0) {
        return NULL;
    }
    const char *rel = path + root_len;
    while (*rel == '/') {
        rel++;
    }
    return rel;
}

/* Stat WITHOUT following links, so a repository-controlled symlink cannot lead
 * the walk outside the project. The previous stat() resolved the link, reported
 * the target's S_ISDIR, and the walk happily descended wherever it pointed.
 * Returns 0 on success, CBM_NOT_FOUND to skip the entry.
 * Mirrors pass_pkgmap.c's pkgmap_safe_stat and discover.c's safe_stat. */
static int envscan_safe_stat(const char *abs_path, struct stat *st) {
#ifdef _WIN32
    /* Windows has no lstat; a junction or directory symlink surfaces as a
     * reparse point, which is the same escape hatch, so screen the attribute.
     * The wide stat also keeps non-ASCII paths off the ANSI CRT. */
    wchar_t *wide = cbm_path_to_wide(abs_path);
    if (!wide) {
        return CBM_NOT_FOUND;
    }
    DWORD attributes = GetFileAttributesW(wide);
    if (attributes != INVALID_FILE_ATTRIBUTES && (attributes & FILE_ATTRIBUTE_REPARSE_POINT)) {
        free(wide);
        return CBM_NOT_FOUND;
    }
    struct _stat64 wst;
    int ret = _wstat64(wide, &wst);
    free(wide);
    if (ret != 0) {
        return CBM_NOT_FOUND;
    }
    st->st_mode = wst.st_mode;
    st->st_size = wst.st_size;
    st->st_mtime = wst.st_mtime;
    return 0;
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

/* True when `candidate` still resolves to somewhere under `canonical_root`.
 *
 * Belt to envscan_safe_stat's braces. The link screen refuses the symlink
 * itself; this catches escapes a mode check cannot see — a resolved path that
 * leaves the root for any other reason. `canonical_root` is resolved once per
 * scan because cbm_canonical_path costs a syscall per component. */
static bool envscan_within_root(const char *candidate, const char *canonical_root) {
    char resolved[CBM_SZ_4K];
    if (!cbm_canonical_path(candidate, resolved, sizeof(resolved))) {
        return false;
    }
#ifdef _WIN32
    cbm_normalize_path_sep(resolved);
#endif
    size_t root_len = strlen(canonical_root);
    if (root_len == 0 || envscan_path_ncmp(resolved, canonical_root, root_len) != 0) {
        return false;
    }
    /* Demand a real component boundary, so root "/repo" does not admit
     * "/repo-elsewhere". */
    return canonical_root[root_len - 1] == '/' || resolved[root_len] == '\0' ||
           resolved[root_len] == '/';
}

/* ── Public API ────────────────────────────────────────────────── */

/* Open a candidate config file without following a symlink where the platform
 * allows it. Mode "r" is preserved from the previous cbm_fopen call so the
 * per-platform newline handling the line loop already copes with is unchanged. */
static FILE *envscan_open_file(const char *full_path) {
#ifdef _WIN32
    /* No O_NOFOLLOW equivalent here; reparse points were screened by
     * envscan_safe_stat. cbm_fopen is required for UTF-8 → _wfopen mapping. */
    return cbm_fopen(full_path, "r");
#else
    /* O_NOFOLLOW closes the window between the lstat above and this open: if the
     * entry turned into a symlink in between, the open fails rather than reading
     * through it. A raw open is correct on POSIX — paths are bytes there, and the
     * cbm_fopen rule exists for Windows' wide-API mapping. */
    int flags = O_RDONLY | O_CLOEXEC;
#ifdef O_NOFOLLOW
    flags |= O_NOFOLLOW;
#endif
    int descriptor = open(full_path, flags);
    if (descriptor < 0) {
        return NULL;
    }
    FILE *stream = fdopen(descriptor, "r");
    if (!stream) {
        (void)close(descriptor);
    }
    return stream;
#endif
}

/* Scan a single file for env URL bindings. Returns number of bindings added. */
static int scan_env_file(const char *full_path, const char *rel, file_type_t ft,
                         cbm_env_binding_t *out, int max_out) {
    FILE *f = envscan_open_file(full_path);
    if (!f) {
        return 0;
    }

    struct stat fst;
    if (fstat(fileno(f), &fst) != 0 || fst.st_size > (long)CBM_SZ_1K * CBM_SZ_1K) {
        (void)fclose(f);
        return 0;
    }

    int count = 0;
    char line[CBM_SZ_2K];
    while (fgets(line, sizeof(line), f) && count < max_out) {
        size_t ll = strlen(line);
        while (ll > 0 && (line[ll - SKIP_ONE] == '\n' || line[ll - SKIP_ONE] == '\r')) {
            line[--ll] = '\0';
        }

        char key[CBM_SZ_128];
        char value[CBM_SZ_512];
        if (!scan_line(line, ft, key, sizeof(key), value, sizeof(value))) {
            continue;
        }
        if (strncmp(value, "http://", SLEN("http://")) != 0 &&
            strncmp(value, "https://", SLEN("https://")) != 0) {
            continue;
        }
        if (cbm_is_secret_binding(key, value) || cbm_is_secret_value(value)) {
            continue;
        }

        strncpy(out[count].key, key, sizeof(out[count].key) - 1);
        out[count].key[sizeof(out[count].key) - SKIP_ONE] = '\0';
        strncpy(out[count].value, value, sizeof(out[count].value) - 1);
        out[count].value[sizeof(out[count].value) - SKIP_ONE] = '\0';
        strncpy(out[count].file_path, rel, sizeof(out[count].file_path) - 1);
        out[count].file_path[sizeof(out[count].file_path) - SKIP_ONE] = '\0';
        count++;
    }
    (void)fclose(f);
    return count;
}

/* Process a single directory entry for env scanning. Returns bindings added.
 * `root_len` is strlen of the root the walk started from, verified against
 * path_stack[0] by the caller; `canonical_root` is that root resolved once. */
static int process_env_entry(cbm_dirent_t *ent, const char *dir_path, const char *root_path,
                             size_t root_len, const char *canonical_root, cbm_env_binding_t *out,
                             int max_out, char path_stack[][CBM_SZ_512], int *stack_top,
                             char **excluded_dirs, int excluded_count) {
    char full_path[CBM_SZ_512];
    if (!envscan_join_path(full_path, sizeof(full_path), dir_path, ent->name)) {
        return 0;
    }
    const char *rel = envscan_relative_suffix(full_path, root_path, root_len);
    if (!rel) {
        return 0;
    }

    struct stat st;
    if (envscan_safe_stat(full_path, &st) != 0) {
        return 0;
    }
    if (S_ISDIR(st.st_mode)) {
        if (is_ignored_dir(ent->name) ||
            cbm_pipeline_relpath_is_excluded(rel, excluded_dirs, excluded_count) ||
            *stack_top >= CBM_SZ_256) {
            return 0;
        }
        if (!envscan_within_root(full_path, canonical_root)) {
            cbm_log_warn("envscan.dir_outside_root", "path", full_path, "root", canonical_root);
            return 0;
        }
        /* Checked copy. The stack rows are the same width as full_path, so this
         * cannot truncate today — but check anyway rather than lean on that
         * coincidence: a truncated row is exactly what reintroduced the
         * out-of-bounds suffix arithmetic. */
        int written = snprintf(path_stack[*stack_top], sizeof(path_stack[0]), "%s", full_path);
        if (written <= 0 || (size_t)written >= sizeof(path_stack[0])) {
            cbm_log_warn("envscan.path_too_long", "dir", dir_path, "name", ent->name);
            return 0;
        }
        (*stack_top)++;
        return 0;
    }
    /* Only regular files are config files. Without this, the lstat above still
     * lets a FIFO through, and opening one blocks the pass until a writer
     * appears. */
    if (!S_ISREG(st.st_mode)) {
        return 0;
    }
    if (is_secret_file(ent->name)) {
        return 0;
    }
    file_type_t ft = detect_file_type(ent->name);
    if (ft == FT_UNKNOWN) {
        return 0;
    }
    return scan_env_file(full_path, rel, ft, out, max_out);
}

int cbm_scan_project_env_urls_excluded(const char *root_path, cbm_env_binding_t *out, int max_out,
                                       char **excluded_dirs, int excluded_count) {
    if (!root_path || !out || max_out <= 0) {
        return 0;
    }
    compile_patterns();

    int count = 0;
    char path_stack[CBM_SZ_256][CBM_SZ_512];
    int stack_top = SKIP_ONE;
    /* Reject an over-long root instead of truncating it in. The old strncpy left
     * path_stack[0] SHORTER than root_path, and every suffix pointer derived from
     * strlen(root_path) was then past the end of its buffer. Establishing
     * root_len from the copy that actually fit is what removes that class. */
    int written = snprintf(path_stack[0], sizeof(path_stack[0]), "%s", root_path);
    if (written <= 0 || (size_t)written >= sizeof(path_stack[0])) {
        cbm_log_warn("envscan.root_too_long", "root", root_path);
        return 0;
    }
    size_t root_len = (size_t)written;

    /* Resolved once per scan: every directory the walk wants to descend is
     * checked against this, and resolving costs a syscall per component. */
    char canonical_root[CBM_SZ_4K];
    if (!cbm_canonical_path(root_path, canonical_root, sizeof(canonical_root))) {
        /* Unresolvable root — nothing to walk. Previously opendir simply failed
         * on the first iteration, so the result is the same: zero bindings. */
        return 0;
    }
#ifdef _WIN32
    cbm_normalize_path_sep(canonical_root);
#endif

    while (stack_top > 0 && count < max_out) {
        stack_top--;
        char dir_path[CBM_SZ_512];
        int copied = snprintf(dir_path, sizeof(dir_path), "%s", path_stack[stack_top]);
        if (copied <= 0 || (size_t)copied >= sizeof(dir_path)) {
            continue;
        }

        cbm_dir_t *d = cbm_opendir(dir_path);
        if (!d) {
            continue;
        }
        cbm_dirent_t *ent;
        while ((ent = cbm_readdir(d)) && count < max_out) {
            count += process_env_entry(ent, dir_path, root_path, root_len, canonical_root,
                                       out + count, max_out - count, path_stack, &stack_top,
                                       excluded_dirs, excluded_count);
        }
        cbm_closedir(d);
    }
    return count;
}

int cbm_scan_project_env_urls(const char *root_path, cbm_env_binding_t *out, int max_out) {
    return cbm_scan_project_env_urls_excluded(root_path, out, max_out, NULL, 0);
}
