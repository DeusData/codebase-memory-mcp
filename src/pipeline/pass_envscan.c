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
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

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

/* ── Public API ────────────────────────────────────────────────── */

/* Scan a single file for env URL bindings. Returns number of bindings added. */
static int scan_env_file(const char *full_path, const char *rel, file_type_t ft,
                         cbm_env_binding_t *out, int max_out) {
    FILE *f = fopen(full_path, "r");
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

/* Process a single directory entry for env scanning. Returns bindings added. */
static int process_env_entry(cbm_dirent_t *ent, const char *dir_path, const char *root_path,
                             cbm_env_binding_t *out, int max_out, char path_stack[][CBM_SZ_512],
                             int *stack_top) {
    char full_path[CBM_SZ_512];
    snprintf(full_path, sizeof(full_path), "%s/%s", dir_path, ent->name);

    struct stat st;
    if (stat(full_path, &st) != 0) {
        return 0;
    }
    if (S_ISDIR(st.st_mode)) {
        if (!is_ignored_dir(ent->name) && *stack_top < CBM_SZ_256) {
            strncpy(path_stack[*stack_top], full_path, sizeof(path_stack[0]) - 1);
            path_stack[*stack_top][sizeof(path_stack[0]) - SKIP_ONE] = '\0';
            (*stack_top)++;
        }
        return 0;
    }
    if (is_secret_file(ent->name)) {
        return 0;
    }
    file_type_t ft = detect_file_type(ent->name);
    if (ft == FT_UNKNOWN) {
        return 0;
    }
    const char *rel = full_path + strlen(root_path);
    while (*rel == '/') {
        rel++;
    }
    return scan_env_file(full_path, rel, ft, out, max_out);
}

int cbm_scan_project_env_urls(const char *root_path, cbm_env_binding_t *out, int max_out) {
    if (!root_path || !out || max_out <= 0) {
        return 0;
    }
    compile_patterns();

    int count = 0;
    char path_stack[CBM_SZ_256][CBM_SZ_512];
    int stack_top = SKIP_ONE;
    strncpy(path_stack[0], root_path, sizeof(path_stack[0]) - 1);
    path_stack[0][sizeof(path_stack[0]) - SKIP_ONE] = '\0';

    while (stack_top > 0 && count < max_out) {
        stack_top--;
        char dir_path[CBM_SZ_512];
        strncpy(dir_path, path_stack[stack_top], sizeof(dir_path) - SKIP_ONE);
        dir_path[sizeof(dir_path) - SKIP_ONE] = '\0';

        cbm_dir_t *d = cbm_opendir(dir_path);
        if (!d) {
            continue;
        }
        cbm_dirent_t *ent;
        while ((ent = cbm_readdir(d)) && count < max_out) {
            count += process_env_entry(ent, dir_path, root_path, out + count, max_out - count,
                                       path_stack, &stack_top);
        }
        cbm_closedir(d);
    }
    return count;
}
