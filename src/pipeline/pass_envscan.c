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
#include "pipeline/walk_path.h"
#include "foundation/compat.h"
#include "foundation/compat_thread.h"
#include "foundation/hash_table.h"
#include "foundation/limits.h"
#include "foundation/log.h"
#include "foundation/str_util.h"

#include <ctype.h>
#include "foundation/compat_fs.h"
#include "foundation/compat_regex.h"
#include "foundation/platform.h" /* cbm_normalize_path_sep */
#include <inttypes.h>
#include <limits.h>
#include <stdatomic.h>
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
static cbm_mutex_t patterns_mutex;

enum {
    ENVSCAN_PATTERN_MUTEX_UNINITIALIZED = 0,
    ENVSCAN_PATTERN_MUTEX_INITIALIZING = 1,
    ENVSCAN_PATTERN_MUTEX_INITIALIZED = 2,
    ENVSCAN_PATTERN_MUTEX_WAIT_MICROSECONDS = 1000,
};

static atomic_int patterns_mutex_state = ENVSCAN_PATTERN_MUTEX_UNINITIALIZED;

static void envscan_patterns_mutex_initialize(void) {
    int state = atomic_load(&patterns_mutex_state);
    if (state == ENVSCAN_PATTERN_MUTEX_INITIALIZED) {
        return;
    }
    state = ENVSCAN_PATTERN_MUTEX_UNINITIALIZED;
    if (atomic_compare_exchange_strong(&patterns_mutex_state, &state,
                                       ENVSCAN_PATTERN_MUTEX_INITIALIZING)) {
        cbm_mutex_init(&patterns_mutex);
        atomic_store(&patterns_mutex_state, ENVSCAN_PATTERN_MUTEX_INITIALIZED);
        return;
    }
    while (atomic_load(&patterns_mutex_state) != ENVSCAN_PATTERN_MUTEX_INITIALIZED) {
        cbm_usleep(ENVSCAN_PATTERN_MUTEX_WAIT_MICROSECONDS);
    }
}

/* POSIX ERE doesn't support \w or \S — use bracket expressions */
#define W "[A-Za-z0-9_]" /* word char */
#define NW "[^ \t\"']"   /* non-whitespace, non-quote */

static void compile_patterns(void) {
    envscan_patterns_mutex_initialize();
    cbm_mutex_lock(&patterns_mutex);
    if (patterns_compiled) {
        cbm_mutex_unlock(&patterns_mutex);
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
    cbm_mutex_unlock(&patterns_mutex);
}

/* Free all compiled regex patterns. Safe to call even if never compiled.
 * Call this in test teardown or at process exit to suppress leak reports. */
void cbm_envscan_free_patterns(void) {
    if (atomic_load(&patterns_mutex_state) != ENVSCAN_PATTERN_MUTEX_INITIALIZED) {
        return;
    }
    cbm_mutex_lock(&patterns_mutex);
    if (!patterns_compiled) {
        cbm_mutex_unlock(&patterns_mutex);
        return;
    }
    cbm_regfree(&dockerfile_re);
    cbm_regfree(&yaml_kv_re);
    cbm_regfree(&yaml_setenv_re);
    cbm_regfree(&terraform_re);
    cbm_regfree(&shell_re);
    cbm_regfree(&envfile_re);
    cbm_regfree(&toml_re);
    cbm_regfree(&properties_re);
    patterns_compiled = 0;
    cbm_mutex_unlock(&patterns_mutex);
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

typedef enum {
    ENVSCAN_LINE_NO_MATCH = 0,
    ENVSCAN_LINE_MATCH = 1,
    ENVSCAN_LINE_KEY_UNREPRESENTABLE = 2,
    ENVSCAN_LINE_VALUE_UNREPRESENTABLE = 3,
} envscan_line_result_t;

/* Extract key/value from a regex match with two capture groups.
 * Oversized fields are distinguished from no-match so the caller can explain
 * the fixed output API's representational boundary without logging values. */
static envscan_line_result_t extract_kv_groups(const char *trimmed, const cbm_regmatch_t *m,
                                               int key_grp, int val_grp, char *key_out,
                                               size_t key_sz, char *val_out, size_t val_sz) {
    int klen = (m[key_grp].rm_eo - m[key_grp].rm_so);
    int vlen = (m[val_grp].rm_eo - m[val_grp].rm_so);
    if (klen <= 0 || vlen <= 0) {
        return ENVSCAN_LINE_NO_MATCH;
    }
    if ((size_t)klen >= key_sz) {
        return ENVSCAN_LINE_KEY_UNREPRESENTABLE;
    }
    if ((size_t)vlen >= val_sz) {
        return ENVSCAN_LINE_VALUE_UNREPRESENTABLE;
    }
    memcpy(key_out, trimmed + m[key_grp].rm_so, klen);
    key_out[klen] = '\0';
    memcpy(val_out, trimmed + m[val_grp].rm_so, vlen);
    val_out[vlen] = '\0';
    return ENVSCAN_LINE_MATCH;
}

/* Try to scan a Dockerfile line. */
static envscan_line_result_t scan_dockerfile_line(const char *line, char *key, size_t ksz,
                                                  char *val, size_t vsz) {
    cbm_regmatch_t m[ENV_REGEX_MAX];
    if (cbm_regexec(&dockerfile_re, line, ENV_GRP_4, m, 0) != 0) {
        return ENVSCAN_LINE_NO_MATCH;
    }
    envscan_line_result_t result =
        extract_kv_groups(line, m, ENV_GRP_2, ENV_GRP_3, key, ksz, val, vsz);
    if (result != ENVSCAN_LINE_MATCH) {
        return result;
    }
    size_t vl = strlen(val);
    while (vl > 0 && (val[vl - SKIP_ONE] == '"' || val[vl - SKIP_ONE] == '\'')) {
        val[--vl] = '\0';
    }
    return ENVSCAN_LINE_MATCH;
}

/* Try to scan a YAML line. */
static envscan_line_result_t scan_yaml_line(const char *line, char *key, size_t ksz, char *val,
                                            size_t vsz) {
    cbm_regmatch_t m[ENV_REGEX_MAX];
    if (cbm_regexec(&yaml_kv_re, line, ENV_GRP_3, m, 0) == 0) {
        envscan_line_result_t result =
            extract_kv_groups(line, m, ENV_GRP_1, ENV_GRP_2, key, ksz, val, vsz);
        if (result != ENVSCAN_LINE_NO_MATCH) {
            return result;
        }
    }
    if (cbm_regexec(&yaml_setenv_re, line, ENV_GRP_3, m, 0) == 0) {
        return extract_kv_groups(line, m, ENV_GRP_1, ENV_GRP_2, key, ksz, val, vsz);
    }
    return ENVSCAN_LINE_NO_MATCH;
}

/* Try to scan a Terraform line. */
static envscan_line_result_t scan_terraform_line(const char *line, char *key, size_t ksz, char *val,
                                                 size_t vsz) {
    cbm_regmatch_t m[ENV_REGEX_MAX];
    if (cbm_regexec(&terraform_re, line, ENV_GRP_3, m, 0) != 0) {
        return ENVSCAN_LINE_NO_MATCH;
    }
    int vlen = (m[ENV_GRP_2].rm_eo - m[ENV_GRP_2].rm_so);
    if (vlen <= 0) {
        return ENVSCAN_LINE_NO_MATCH;
    }
    if ((size_t)vlen >= vsz) {
        return ENVSCAN_LINE_VALUE_UNREPRESENTABLE;
    }
    cbm_str_copy(key, ksz, "_tf_default");
    memcpy(val, line + m[ENV_GRP_2].rm_so, vlen);
    val[vlen] = '\0';
    return ENVSCAN_LINE_MATCH;
}

/* Try single-regex scan (shell, envfile, toml, properties). */
static envscan_line_result_t scan_regex_line(cbm_regex_t *re, const char *line, int kg, int vg,
                                             char *key, size_t ksz, char *val, size_t vsz) {
    cbm_regmatch_t m[ENV_REGEX_MAX];
    if (cbm_regexec(re, line, ENV_GRP_5, m, 0) == 0) {
        return extract_kv_groups(line, m, kg, vg, key, ksz, val, vsz);
    }
    return ENVSCAN_LINE_NO_MATCH;
}

static envscan_line_result_t scan_line(const char *line, file_type_t ft, char *key_out,
                                       size_t key_sz, char *val_out, size_t val_sz) {
    const char *trimmed = line;
    while (*trimmed == ' ' || *trimmed == '\t') {
        trimmed++;
    }
    if (*trimmed == '#' || (trimmed[0] == '/' && trimmed[SKIP_ONE] == '/')) {
        return ENVSCAN_LINE_NO_MATCH;
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
        return ENVSCAN_LINE_NO_MATCH;
    }
}

/* ── Containment ───────────────────────────────────────────────── */

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
    char inline_resolved[CBM_SZ_4K];
    char *resolved = inline_resolved;
    bool resolved_owned = false;
    if (strlen(candidate) >= sizeof(inline_resolved) ||
        !cbm_canonical_path(candidate, inline_resolved, sizeof(inline_resolved))) {
        resolved = cbm_canonical_path_alloc(candidate);
        resolved_owned = true;
    }
    if (!resolved) {
        return false;
    }
#ifdef _WIN32
    cbm_normalize_path_sep(resolved);
#endif
    size_t root_len = strlen(canonical_root);
    if (root_len == 0 || envscan_path_ncmp(resolved, canonical_root, root_len) != 0) {
        if (resolved_owned) {
            free(resolved);
        }
        return false;
    }
    /* Demand a real component boundary, so root "/repo" does not admit
     * "/repo-elsewhere". */
    bool contained = canonical_root[root_len - 1] == '/' || resolved[root_len] == '\0' ||
                     resolved[root_len] == '/';
    if (resolved_owned) {
        free(resolved);
    }
    return contained;
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

static void envscan_log_file_skip(const char *path, const char *reason, const char *constraint,
                                  int64_t file_bytes, int64_t read_bytes, int64_t limit_bytes) {
    char file_buf[CBM_SZ_32];
    char read_buf[CBM_SZ_32];
    char limit_buf[CBM_SZ_32];
    snprintf(file_buf, sizeof(file_buf), "%" PRId64, file_bytes);
    snprintf(read_buf, sizeof(read_buf), "%" PRId64, read_bytes);
    snprintf(limit_buf, sizeof(limit_buf), "%" PRId64, limit_bytes);
    cbm_log_warn("envscan.file_skipped", "path", path, "reason", reason, "constraint", constraint,
                 "file_bytes", file_buf, "read_bytes", read_buf, "limit_bytes", limit_buf);
}

/* Scan one complete logical line at a time. cbm_getline is the repository's
 * portable exact line reader, so a URL spanning the old 2 KiB chunk boundary
 * is neither dropped nor parsed as two different lines. A single reusable line
 * buffer costs O(L) live memory for longest line L; file work is O(B + R), for
 * B bytes and delegated regex cost R. The shared file policy bounds B and
 * growth races after fstat. */
static int scan_env_file(const char *full_path, const char *rel, file_type_t ft,
                         cbm_env_binding_t *out, int max_out) {
    FILE *f = envscan_open_file(full_path);
    if (!f) {
        envscan_log_file_skip(full_path, "open_failed", "filesystem", -1, -1, -1);
        return 0;
    }

    struct stat fst;
    long file_limit = cbm_max_file_bytes();
    if (fstat(cbm_fileno(f), &fst) != 0) {
        (void)fclose(f);
        envscan_log_file_skip(full_path, "size_failed", "filesystem", -1, -1, file_limit);
        return 0;
    }
    if (fst.st_size < 0 || fst.st_size > file_limit) {
        int64_t file_bytes = fst.st_size < 0 ? -1 : (int64_t)fst.st_size;
        (void)fclose(f);
        envscan_log_file_skip(full_path, "oversized", "CBM_MAX_FILE_BYTES", file_bytes, 0,
                              file_limit);
        return 0;
    }

    int count = 0;
    int64_t bytes_read = 0;
    char *line = NULL;
    size_t line_capacity = 0;
    ssize_t line_length;
    while (count < max_out && (line_length = cbm_getline(&line, &line_capacity, f)) >= 0) {
        if ((uint64_t)line_length > (uint64_t)(file_limit - bytes_read)) {
            envscan_log_file_skip(full_path, "grew_oversized", "CBM_MAX_FILE_BYTES",
                                  (int64_t)fst.st_size, bytes_read + line_length, file_limit);
            break;
        }
        bytes_read += line_length;
        size_t ll = (size_t)line_length;
        while (ll > 0 && (line[ll - SKIP_ONE] == '\n' || line[ll - SKIP_ONE] == '\r')) {
            line[--ll] = '\0';
        }

        char key[CBM_SZ_128];
        char value[CBM_SZ_512];
        envscan_line_result_t line_result =
            scan_line(line, ft, key, sizeof(key), value, sizeof(value));
        if (line_result != ENVSCAN_LINE_MATCH) {
            if (line_result == ENVSCAN_LINE_KEY_UNREPRESENTABLE) {
                envscan_log_file_skip(full_path, "binding_unrepresentable", "key_capacity",
                                      (int64_t)fst.st_size, bytes_read, (int64_t)sizeof(key) - 1);
            } else if (line_result == ENVSCAN_LINE_VALUE_UNREPRESENTABLE) {
                envscan_log_file_skip(full_path, "binding_unrepresentable", "value_capacity",
                                      (int64_t)fst.st_size, bytes_read, (int64_t)sizeof(value) - 1);
            }
            continue;
        }
        if (strncmp(value, "http://", SLEN("http://")) != 0 &&
            strncmp(value, "https://", SLEN("https://")) != 0) {
            continue;
        }
        if (cbm_is_secret_binding(key, value) || cbm_is_secret_value(value)) {
            continue;
        }
        if (strlen(rel) >= sizeof(out[count].file_path)) {
            envscan_log_file_skip(full_path, "binding_unrepresentable", "file_path_capacity",
                                  (int64_t)fst.st_size, bytes_read,
                                  (int64_t)sizeof(out[count].file_path) - 1);
            continue;
        }

        cbm_str_copy(out[count].key, sizeof(out[count].key), key);
        cbm_str_copy(out[count].value, sizeof(out[count].value), value);
        cbm_str_copy(out[count].file_path, sizeof(out[count].file_path), rel);
        count++;
    }
    if (ferror(f)) {
        envscan_log_file_skip(full_path, "read_failed", "filesystem_consistency",
                              (int64_t)fst.st_size, bytes_read, file_limit);
    }
    free(line);
    (void)fclose(f);
    return count;
}

/* Process a single directory entry for env scanning. Returns bindings added.
 * `root_len` is strlen of the root the walk started from, verified against
 * path_stack[0] by the caller; `canonical_root` is that root resolved once. */
enum {
    ENVSCAN_WALK_STACK_INITIAL_CAPACITY = 16,
    ENVSCAN_WALK_STACK_GROWTH_FACTOR = 2,
    ENVSCAN_IDENTITY_HEX_LENGTH = (int)(sizeof(uint64_t) * 2U),
    ENVSCAN_IDENTITY_KEY_LENGTH = ENVSCAN_IDENTITY_HEX_LENGTH * 2 + 1,
    ENVSCAN_IDENTITY_KEY_CAPACITY = ENVSCAN_IDENTITY_KEY_LENGTH + 1,
};

typedef struct {
    cbm_dir_t *dir;
    size_t abs_parent_length;
    size_t rel_parent_length;
    char *identity_key;
} envscan_walk_frame_t;

typedef struct {
    envscan_walk_frame_t *frames;
    size_t count;
    size_t capacity;
#ifndef _WIN32
    CBMHashTable *active_identities;
#endif
} envscan_walk_stack_t;

typedef enum {
    ENVSCAN_WALK_PUSH_FAILED = -1,
    ENVSCAN_WALK_PUSH_CYCLE = 0,
    ENVSCAN_WALK_PUSHED = 1,
} envscan_walk_push_result_t;

#ifndef _WIN32
static char envscan_active_identity_present;

static bool envscan_identity_key(const cbm_file_identity_t *identity,
                                 char key[ENVSCAN_IDENTITY_KEY_CAPACITY]) {
    if (!identity || !identity->valid) {
        return false;
    }
    int written = snprintf(key, ENVSCAN_IDENTITY_KEY_CAPACITY, "%0*" PRIx64 ":%0*" PRIx64,
                           ENVSCAN_IDENTITY_HEX_LENGTH, identity->volume,
                           ENVSCAN_IDENTITY_HEX_LENGTH, identity->file);
    return written == ENVSCAN_IDENTITY_KEY_LENGTH;
}
#endif

static envscan_walk_push_result_t envscan_walk_stack_push(envscan_walk_stack_t *stack,
                                                          cbm_dir_t *dir, size_t abs_parent_length,
                                                          size_t rel_parent_length,
                                                          const cbm_file_identity_t *identity) {
    if (!stack || !dir) {
        return ENVSCAN_WALK_PUSH_FAILED;
    }
    char *owned_identity_key = NULL;
#ifndef _WIN32
    char identity_key[ENVSCAN_IDENTITY_KEY_CAPACITY];
    if (!stack->active_identities || !envscan_identity_key(identity, identity_key)) {
        return ENVSCAN_WALK_PUSH_FAILED;
    }
    if (cbm_ht_has(stack->active_identities, identity_key)) {
        return ENVSCAN_WALK_PUSH_CYCLE;
    }
    owned_identity_key = cbm_strdup(identity_key);
    if (!owned_identity_key) {
        return ENVSCAN_WALK_PUSH_FAILED;
    }
#else
    (void)identity;
#endif
    if (stack->count == stack->capacity) {
        size_t new_capacity = stack->capacity == 0
                                  ? ENVSCAN_WALK_STACK_INITIAL_CAPACITY
                                  : stack->capacity * ENVSCAN_WALK_STACK_GROWTH_FACTOR;
        if (new_capacity < stack->capacity ||
            new_capacity > SIZE_MAX / sizeof(envscan_walk_frame_t)) {
            free(owned_identity_key);
            return ENVSCAN_WALK_PUSH_FAILED;
        }
        envscan_walk_frame_t *grown = realloc(stack->frames, new_capacity * sizeof(*grown));
        if (!grown) {
            free(owned_identity_key);
            return ENVSCAN_WALK_PUSH_FAILED;
        }
        stack->frames = grown;
        stack->capacity = new_capacity;
    }
#ifndef _WIN32
    cbm_ht_set(stack->active_identities, owned_identity_key, &envscan_active_identity_present);
    if (!cbm_ht_has(stack->active_identities, owned_identity_key)) {
        free(owned_identity_key);
        return ENVSCAN_WALK_PUSH_FAILED;
    }
#endif
    stack->frames[stack->count++] = (envscan_walk_frame_t){
        .dir = dir,
        .abs_parent_length = abs_parent_length,
        .rel_parent_length = rel_parent_length,
        .identity_key = owned_identity_key,
    };
    return ENVSCAN_WALK_PUSHED;
}

static void envscan_walk_stack_pop(envscan_walk_stack_t *stack, cbm_walk_path_t *abs_path,
                                   cbm_walk_path_t *rel_path) {
    if (!stack || stack->count == 0) {
        return;
    }
    envscan_walk_frame_t *frame = &stack->frames[stack->count - 1U];
    cbm_closedir(frame->dir);
#ifndef _WIN32
    if (frame->identity_key) {
        (void)cbm_ht_delete(stack->active_identities, frame->identity_key);
    }
#endif
    free(frame->identity_key);
    cbm_walk_path_restore(abs_path, frame->abs_parent_length);
    cbm_walk_path_restore(rel_path, frame->rel_parent_length);
    stack->count--;
}

static void envscan_walk_stack_free(envscan_walk_stack_t *stack, cbm_walk_path_t *abs_path,
                                    cbm_walk_path_t *rel_path) {
    if (!stack) {
        return;
    }
    while (stack->count > 0) {
        envscan_walk_stack_pop(stack, abs_path, rel_path);
    }
#ifndef _WIN32
    cbm_ht_free(stack->active_identities);
#endif
    free(stack->frames);
    memset(stack, 0, sizeof(*stack));
}

int cbm_scan_project_env_urls_excluded(const char *root_path, cbm_env_binding_t *out, int max_out,
                                       char **excluded_dirs, int excluded_count) {
    if (!root_path || !out || max_out <= 0) {
        return 0;
    }
    compile_patterns();

    cbm_walk_path_t abs_path = {0};
    cbm_walk_path_t rel_path = {0};
    if (!cbm_walk_path_init(&abs_path, root_path) || !cbm_walk_path_init(&rel_path, "")) {
        cbm_walk_path_free(&rel_path);
        cbm_walk_path_free(&abs_path);
        cbm_log_warn("envscan.walk_skipped", "path", root_path, "reason", "path_allocation_failed");
        return 0;
    }
    char *canonical_root = cbm_canonical_path_alloc(root_path);
    if (!canonical_root) {
        cbm_walk_path_free(&rel_path);
        cbm_walk_path_free(&abs_path);
        return 0;
    }
#ifdef _WIN32
    cbm_normalize_path_sep(canonical_root);
#endif

    envscan_walk_stack_t stack = {0};
#ifndef _WIN32
    stack.active_identities = cbm_ht_create(ENVSCAN_WALK_STACK_INITIAL_CAPACITY);
#endif
    cbm_file_identity_t root_identity = {0};
#ifndef _WIN32
    bool root_identity_ok = cbm_file_identity_read(abs_path.data, &root_identity);
#else
    bool root_identity_ok = true;
#endif
    cbm_dir_t *root = root_identity_ok ? cbm_opendir(abs_path.data) : NULL;
    envscan_walk_push_result_t root_push =
        root ? envscan_walk_stack_push(&stack, root, abs_path.length, rel_path.length,
                                       &root_identity)
             : ENVSCAN_WALK_PUSH_FAILED;
    if (root_push != ENVSCAN_WALK_PUSHED) {
        if (root) {
            cbm_closedir(root);
        }
        cbm_log_warn("envscan.walk_skipped", "path", root_path, "reason",
                     "root_open_identity_or_stack_failed");
        free(canonical_root);
        envscan_walk_stack_free(&stack, &abs_path, &rel_path);
        cbm_walk_path_free(&rel_path);
        cbm_walk_path_free(&abs_path);
        return 0;
    }

    /* Exact iterative DFS. Each accepted descent is a no-follow directory edge;
     * POSIX active identities reject alias/bind cycles and Windows rejects
     * reparse points. Runtime is expected O(E + N + C + B + R), for visited
     * entries E, name bytes N, canonical-containment work C, scanned bytes B,
     * and regex work R. C is filesystem-dependent and can be O(E * P) when
     * resolving every directory path of maximum length P; retaining it preserves
     * the inherited escape check. Live auxiliary memory is O(D + P + L): active
     * depth/handles D, longest paths P, and longest logical line L. */
    int count = 0;
    while (stack.count > 0 && count < max_out) {
        envscan_walk_frame_t *frame = &stack.frames[stack.count - 1U];
        cbm_dirent_t *entry = cbm_readdir(frame->dir);
        if (!entry) {
            envscan_walk_stack_pop(&stack, &abs_path, &rel_path);
            continue;
        }
        const char *name = entry->name;
        if (name[0] == '.' && (name[1] == '\0' || (name[1] == '.' && name[2] == '\0'))) {
            continue;
        }
        size_t abs_parent_length = abs_path.length;
        size_t rel_parent_length = rel_path.length;
        if (!cbm_walk_path_append(&abs_path, name) || !cbm_walk_path_append(&rel_path, name)) {
            cbm_walk_path_restore(&abs_path, abs_parent_length);
            cbm_walk_path_restore(&rel_path, rel_parent_length);
            cbm_log_warn("envscan.walk_entry_skipped", "dir", abs_path.data, "entry", name,
                         "reason", "path_allocation_failed");
            continue;
        }

        struct stat state;
        int stat_result = envscan_safe_stat(abs_path.data, &state);
        if (stat_result == 0 && S_ISDIR(state.st_mode)) {
            bool ignored = is_ignored_dir(name);
            bool excluded =
                cbm_pipeline_relpath_is_excluded(rel_path.data, excluded_dirs, excluded_count);
            bool contained =
                !ignored && !excluded && envscan_within_root(abs_path.data, canonical_root);
            if (contained) {
                cbm_file_identity_t identity = {0};
#ifndef _WIN32
                identity.volume = (uint64_t)state.st_dev;
                identity.file = (uint64_t)state.st_ino;
                identity.valid = true;
#endif
                cbm_dir_t *child = cbm_opendir(abs_path.data);
                envscan_walk_push_result_t pushed =
                    child ? envscan_walk_stack_push(&stack, child, abs_parent_length,
                                                    rel_parent_length, &identity)
                          : ENVSCAN_WALK_PUSH_FAILED;
                if (pushed == ENVSCAN_WALK_PUSHED) {
                    continue;
                }
                if (child) {
                    cbm_closedir(child);
                }
                cbm_log_warn("envscan.walk_entry_skipped", "path", abs_path.data, "reason",
                             pushed == ENVSCAN_WALK_PUSH_CYCLE ? "directory_cycle"
                                                               : "directory_open_or_stack_failed");
            } else if (!ignored && !excluded) {
                cbm_log_warn("envscan.dir_outside_root", "path", abs_path.data, "root",
                             canonical_root);
            }
        } else if (stat_result == 0 && S_ISREG(state.st_mode) && !is_secret_file(name)) {
            file_type_t file_type = detect_file_type(name);
            if (file_type != FT_UNKNOWN) {
                count += scan_env_file(abs_path.data, rel_path.data, file_type, out + count,
                                       max_out - count);
            }
        }
        cbm_walk_path_restore(&abs_path, abs_parent_length);
        cbm_walk_path_restore(&rel_path, rel_parent_length);
    }
    free(canonical_root);
    envscan_walk_stack_free(&stack, &abs_path, &rel_path);
    cbm_walk_path_free(&rel_path);
    cbm_walk_path_free(&abs_path);
    return count;
}

int cbm_scan_project_env_urls(const char *root_path, cbm_env_binding_t *out, int max_out) {
    return cbm_scan_project_env_urls_excluded(root_path, out, max_out, NULL, 0);
}
