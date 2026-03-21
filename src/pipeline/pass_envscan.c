/*
 * pass_envscan.c — Environment URL scanner.
 *
 * Walks a project directory, scans config files (Dockerfile, .env, shell,
 * YAML, TOML, Terraform, .properties) for environment variable assignments
 * where the value is a URL. Filters out secrets.
 *
 * Port of internal/pipeline/envscan.go:ScanProjectEnvURLs().
 */
// NOLINTNEXTLINE(misc-include-cleaner) — pipeline.h included for interface contract
#include "pipeline/pipeline.h"
#include "pipeline/pipeline_internal.h"
// NOLINTNEXTLINE(misc-include-cleaner) — log.h included for interface contract
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

    patterns_compiled = 1;
}

#undef W
#undef NW

/* ── File type detection ───────────────────────────────────────── */

static int is_dockerfile_name(const char *name) {
    /* Case-insensitive check */
    char lower[256];
    size_t len = strlen(name);
    if (len >= sizeof(lower)) {
        return 0;
    }
    for (size_t i = 0; i <= len; i++) {
        lower[i] = (char)tolower((unsigned char)name[i]);
    }

    if (strcmp(lower, "dockerfile") == 0) {
        return 1;
    }
#define DOCKERFILE_SUFFIX_LEN 11 /* strlen("dockerfile.") == strlen(".dockerfile") */
    if (strncmp(lower, "dockerfile.", DOCKERFILE_SUFFIX_LEN) == 0) {
        return 1;
    }
    if (len > DOCKERFILE_SUFFIX_LEN &&
        strcmp(lower + len - DOCKERFILE_SUFFIX_LEN, ".dockerfile") == 0) {
        return 1;
    }
    return 0;
}

static int is_env_file_name(const char *name) {
    char lower[256];
    size_t len = strlen(name);
    if (len >= sizeof(lower)) {
        return 0;
    }
    for (size_t i = 0; i <= len; i++) {
        lower[i] = (char)tolower((unsigned char)name[i]);
    }

    if (strcmp(lower, ".env") == 0) {
        return 1;
    }
    if (strncmp(lower, ".env.", 5) == 0) {
        return 1;
    }
    if (len > 4 && strcmp(lower + len - 4, ".env") == 0) {
        return 1;
    }
    return 0;
}

static int is_secret_file(const char *name) {
    char lower[256];
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
            return 1;
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
            return 1;
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

static int scan_line(const char *line, file_type_t ft, char *key_out, size_t key_sz, char *val_out,
                     size_t val_sz) {
    cbm_regmatch_t m[5];
    const char *trimmed = line;
    while (*trimmed == ' ' || *trimmed == '\t') {
        trimmed++;
    }

    /* Skip comments */
    if (*trimmed == '#' || (trimmed[0] == '/' && trimmed[1] == '/')) {
        return 0;
    }

    switch (ft) {
    case FT_DOCKERFILE:
        if (cbm_regexec(&dockerfile_re, trimmed, 4, m, 0) == 0) {
            /* group 2 = key, group 3 = value */
            int klen = (m[2].rm_eo - m[2].rm_so);
            int vlen = (m[3].rm_eo - m[3].rm_so);
            if (klen > 0 && klen < (int)key_sz && vlen > 0 && vlen < (int)val_sz) {
                memcpy(key_out, trimmed + m[2].rm_so, klen);
                key_out[klen] = '\0';
                memcpy(val_out, trimmed + m[3].rm_so, vlen);
                val_out[vlen] = '\0';
                /* Trim quotes from value */
                size_t vl = strlen(val_out);
                while (vl > 0 && (val_out[vl - 1] == '"' || val_out[vl - 1] == '\'')) {
                    val_out[--vl] = '\0';
                }
                return 1;
            }
        }
        break;

    case FT_YAML:
        if (cbm_regexec(&yaml_kv_re, trimmed, 3, m, 0) == 0) {
            int klen = (m[1].rm_eo - m[1].rm_so);
            int vlen = (m[2].rm_eo - m[2].rm_so);
            if (klen > 0 && klen < (int)key_sz && vlen > 0 && vlen < (int)val_sz) {
                memcpy(key_out, trimmed + m[1].rm_so, klen);
                key_out[klen] = '\0';
                memcpy(val_out, trimmed + m[2].rm_so, vlen);
                val_out[vlen] = '\0';
                return 1;
            }
        }
        if (cbm_regexec(&yaml_setenv_re, trimmed, 3, m, 0) == 0) {
            int klen = (m[1].rm_eo - m[1].rm_so);
            int vlen = (m[2].rm_eo - m[2].rm_so);
            if (klen > 0 && klen < (int)key_sz && vlen > 0 && vlen < (int)val_sz) {
                memcpy(key_out, trimmed + m[1].rm_so, klen);
                key_out[klen] = '\0';
                memcpy(val_out, trimmed + m[2].rm_so, vlen);
                val_out[vlen] = '\0';
                return 1;
            }
        }
        break;

    case FT_TERRAFORM:
        if (cbm_regexec(&terraform_re, trimmed, 3, m, 0) == 0) {
            int vlen = (m[2].rm_eo - m[2].rm_so);
            if (vlen > 0 && vlen < (int)val_sz) {
                strncpy(key_out, "_tf_default", key_sz - 1);
                key_out[key_sz - 1] = '\0';
                memcpy(val_out, trimmed + m[2].rm_so, vlen);
                val_out[vlen] = '\0';
                return 1;
            }
        }
        break;

    case FT_SHELL:
        if (cbm_regexec(&shell_re, trimmed, 4, m, 0) == 0) {
            /* group 2 = key, group 3 = value */
            int klen = (m[2].rm_eo - m[2].rm_so);
            int vlen = (m[3].rm_eo - m[3].rm_so);
            if (klen > 0 && klen < (int)key_sz && vlen > 0 && vlen < (int)val_sz) {
                memcpy(key_out, trimmed + m[2].rm_so, klen);
                key_out[klen] = '\0';
                memcpy(val_out, trimmed + m[3].rm_so, vlen);
                val_out[vlen] = '\0';
                return 1;
            }
        }
        break;

    case FT_ENVFILE:
        if (cbm_regexec(&envfile_re, trimmed, 3, m, 0) == 0) {
            int klen = (m[1].rm_eo - m[1].rm_so);
            int vlen = (m[2].rm_eo - m[2].rm_so);
            if (klen > 0 && klen < (int)key_sz && vlen > 0 && vlen < (int)val_sz) {
                memcpy(key_out, trimmed + m[1].rm_so, klen);
                key_out[klen] = '\0';
                memcpy(val_out, trimmed + m[2].rm_so, vlen);
                val_out[vlen] = '\0';
                return 1;
            }
        }
        break;

    case FT_TOML:
        if (cbm_regexec(&toml_re, trimmed, 3, m, 0) == 0) {
            int klen = (m[1].rm_eo - m[1].rm_so);
            int vlen = (m[2].rm_eo - m[2].rm_so);
            if (klen > 0 && klen < (int)key_sz && vlen > 0 && vlen < (int)val_sz) {
                memcpy(key_out, trimmed + m[1].rm_so, klen);
                key_out[klen] = '\0';
                memcpy(val_out, trimmed + m[2].rm_so, vlen);
                val_out[vlen] = '\0';
                return 1;
            }
        }
        break;

    case FT_PROPERTIES:
        if (cbm_regexec(&properties_re, trimmed, 3, m, 0) == 0) {
            int klen = (m[1].rm_eo - m[1].rm_so);
            int vlen = (m[2].rm_eo - m[2].rm_so);
            if (klen > 0 && klen < (int)key_sz && vlen > 0 && vlen < (int)val_sz) {
                memcpy(key_out, trimmed + m[1].rm_so, klen);
                key_out[klen] = '\0';
                memcpy(val_out, trimmed + m[2].rm_so, vlen);
                val_out[vlen] = '\0';
                return 1;
            }
        }
        break;

    default:
        break;
    }
    return 0;
}

/* ── Public API ────────────────────────────────────────────────── */

int cbm_scan_project_env_urls(const char *root_path, cbm_env_binding_t *out, int max_out) {
    if (!root_path || !out || max_out <= 0) {
        return 0;
    }

    compile_patterns();

    int count = 0;

    /* Recursive directory walk using a stack */
    char path_stack[256][512];
    int stack_top = 0;
    strncpy(path_stack[0], root_path, sizeof(path_stack[0]) - 1);
    path_stack[0][sizeof(path_stack[0]) - 1] = '\0';
    stack_top = 1;

    while (stack_top > 0 && count < max_out) {
        stack_top--;
        char dir_path[512];
        strncpy(dir_path, path_stack[stack_top], sizeof(dir_path) - 1);
        dir_path[sizeof(dir_path) - 1] = '\0';

        cbm_dir_t *d = cbm_opendir(dir_path);
        if (!d) {
            continue;
        }

        cbm_dirent_t *ent;
        while ((ent = cbm_readdir(d)) && count < max_out) {
            char full_path[512];
            snprintf(full_path, sizeof(full_path), "%s/%s", dir_path, ent->name);

            struct stat st;
            if (stat(full_path, &st) != 0) {
                continue;
            }

            if (S_ISDIR(st.st_mode)) {
                if (is_ignored_dir(ent->name)) {
                    continue;
                }
                if (stack_top < 256) {
                    strncpy(path_stack[stack_top], full_path, sizeof(path_stack[0]) - 1);
                    path_stack[stack_top][sizeof(path_stack[0]) - 1] = '\0';
                    stack_top++;
                }
                continue;
            }

            /* Skip secret files */
            if (is_secret_file(ent->name)) {
                continue;
            }

            /* Determine file type */
            file_type_t ft = detect_file_type(ent->name);
            if (ft == FT_UNKNOWN) {
                continue;
            }

            /* Compute relative path */
            const char *rel = full_path + strlen(root_path);
            while (*rel == '/') {
                rel++;
            }

            /* Open first, then fstat on fd to avoid TOCTOU race */
            FILE *f = fopen(full_path, "r");
            if (!f) {
                continue;
            }

            /* Recheck size on the open fd (not the path) */
            struct stat fst;
            if (fstat(fileno(f), &fst) != 0 || fst.st_size > (long)1024 * 1024) {
                fclose(f);
                continue;
            }

            char line[2048];
            while (fgets(line, sizeof(line), f) && count < max_out) {
                /* Strip trailing newline */
                size_t ll = strlen(line);
                while (ll > 0 && (line[ll - 1] == '\n' || line[ll - 1] == '\r')) {
                    line[--ll] = '\0';
                }

                char key[128];
                char value[512];
                if (scan_line(line, ft, key, sizeof(key), value, sizeof(value))) {
                    /* Must be a URL */
                    if (strncmp(value, "http://", 7) != 0 && strncmp(value, "https://", 8) != 0) {
                        continue;
                    }
                    /* Secret exclusion */
                    if (cbm_is_secret_binding(key, value)) {
                        continue;
                    }
                    if (cbm_is_secret_value(value)) {
                        continue;
                    }

                    /* Add binding */
                    strncpy(out[count].key, key, sizeof(out[count].key) - 1);
                    out[count].key[sizeof(out[count].key) - 1] = '\0';
                    strncpy(out[count].value, value, sizeof(out[count].value) - 1);
                    out[count].value[sizeof(out[count].value) - 1] = '\0';
                    strncpy(out[count].file_path, rel, sizeof(out[count].file_path) - 1);
                    out[count].file_path[sizeof(out[count].file_path) - 1] = '\0';
                    count++;
                }
            }
            (void)fclose(f);
        }
        cbm_closedir(d);
    }

    return count;
}
