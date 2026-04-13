/*
 * pass_infrascan.c — Infrastructure file detection and parsing helpers.
 *
 * Pure helper functions for detecting infrastructure file types,
 * secret filtering, and parsing Dockerfiles, .env files, shell scripts,
 * and Terraform files.
 */
#include "foundation/constants.h"

enum {
    IS_ENV_EXT = 4, /* strlen(".env") */
    IS_CONTENT_LIMIT = 4096,
    IS_MAX_STAGES = 16,
    IS_MAX_PORTS = 16,
    IS_MAX_SOURCES = 16,
    IS_TOKEN_MIN_1 = 16,    /* min alnum for AWS key */
    IS_TOKEN_MIN_2 = 20,    /* min alnum for GH classic token */
    IS_FROM_SKIP = 4,       /* strlen("FROM") */
    IS_EXPOSE_SKIP = 6,     /* strlen("EXPOSE") */
    IS_RUN_SKIP = 3,        /* strlen("RUN") / strlen("ENV") / strlen("ARG") */
    IS_CMD_SKIP = 4,        /* strlen("CMD ") */
    IS_USER_SKIP = 5,       /* strlen("USER ") */
    IS_EXPORT_SKIP = 7,     /* strlen("export ") */
    IS_WORKDIR_SKIP = 8,    /* strlen("WORKDIR ") */
    IS_ENTRYPOINT_OFF = 10, /* strlen("ENTRYPOINT") */
    IS_AS_SKIP = 3,         /* strlen("AS ") */
    IS_SOURCE_DOT = 2,      /* strlen(". ") */
    IS_QUOTE_TRIM = 2,      /* strip surrounding quotes */
};
#define IS_NOT_FOUND (-1)

#define SLEN(s) (sizeof(s) - 1)
#include "pipeline/pipeline.h"
#include "pipeline/pipeline_internal.h"

#include <ctype.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

/* ── Constants ───────────────────────────────────────────────────── */

/* String length constants for keyword matching */
#define LEN_DOCKERFILE 11          /* strlen("dockerfile.") == strlen(".dockerfile") */
#define LEN_DOCKER_COMPOSE 14      /* strlen("docker-compose") */
#define LEN_HEALTHCHECK 11         /* strlen("HEALTHCHECK") */
#define LEN_DOCKER_COMPOSE_SKIP 15 /* strlen("docker-compose") + space */

/* Minimum alnum chars after prefix for secret detection */
#define GITHUB_PAT_MIN_ALNUM 36 /* ghp_ + 36 alnum chars */

/* ── Internal helpers ────────────────────────────────────────────── */

static void to_lower(const char *src, char *dst, size_t dst_sz) {
    size_t i;
    size_t len = strlen(src);
    if (len >= dst_sz) {
        len = dst_sz - SKIP_ONE;
    }
    for (i = 0; i < len; i++) {
        dst[i] = (char)tolower((unsigned char)src[i]);
    }
    dst[i] = '\0';
}

/* Case-insensitive substring search */
static const char *ci_strstr(const char *haystack, const char *needle) {
    if (!*needle) {
        return haystack;
    }
    size_t nlen = strlen(needle);
    for (; *haystack; haystack++) {
        if (strncasecmp(haystack, needle, nlen) == 0) {
            return haystack;
        }
    }
    return NULL;
}

/* Count consecutive alphanumeric characters */
static int count_alnum(const char *s) {
    int n = 0;
    while (isalnum((unsigned char)s[n])) {
        n++;
    }
    return n;
}

/* Count consecutive alphanumeric or dash characters */
static int count_alnum_dash(const char *s) {
    int n = 0;
    while (isalnum((unsigned char)s[n]) || s[n] == '-') {
        n++;
    }
    return n;
}

/* Skip whitespace, return pointer past it */
static const char *skip_ws(const char *p) {
    while (*p == ' ' || *p == '\t') {
        p++;
    }
    return p;
}

/* Extract a word ([a-zA-Z0-9_]+) into buf. Returns length. */
static int extract_word(const char *p, char *buf, size_t buf_sz) {
    int n = 0;
    while ((isalnum((unsigned char)p[n]) || p[n] == '_') && (size_t)n < buf_sz - SKIP_ONE) {
        buf[n] = p[n];
        n++;
    }
    buf[n] = '\0';
    return n;
}

/* Extract a non-space token into buf. Returns length. */
static int extract_token(const char *p, char *buf, size_t buf_sz) {
    int n = 0;
    while (p[n] && p[n] != ' ' && p[n] != '\t' && (size_t)n < buf_sz - SKIP_ONE) {
        buf[n] = p[n];
        n++;
    }
    buf[n] = '\0';
    return n;
}

/* Trim trailing whitespace in-place */
static void rtrim(char *s) {
    size_t len = strlen(s);
    while (len > 0 && (s[len - SKIP_ONE] == ' ' || s[len - SKIP_ONE] == '\t' ||
                       s[len - SKIP_ONE] == '\r' || s[len - SKIP_ONE] == '\n')) {
        s[--len] = '\0';
    }
}

/* ── File identification ────────────────────────────────────────── */

bool cbm_is_dockerfile(const char *name) {
    if (!name) {
        return false;
    }
    char lower[CBM_SZ_256];
    to_lower(name, lower, sizeof(lower));

    if (strcmp(lower, "dockerfile") == 0) {
        return true;
    }
    if (strncmp(lower, "dockerfile.", LEN_DOCKERFILE) == 0) {
        return true;
    }
    size_t len = strlen(lower);
    if (len > LEN_DOCKERFILE && strcmp(lower + len - LEN_DOCKERFILE, ".dockerfile") == 0) {
        return true;
    }
    return false;
}

bool cbm_is_compose_file(const char *name) {
    if (!name) {
        return false;
    }
    char lower[CBM_SZ_256];
    to_lower(name, lower, sizeof(lower));

    bool prefix_match = (strncmp(lower, "docker-compose", LEN_DOCKER_COMPOSE) == 0) ||
                        (strcmp(lower, "compose.yml") == 0) || (strcmp(lower, "compose.yaml") == 0);
    if (!prefix_match) {
        return false;
    }

    const char *ext = strrchr(lower, '.');
    if (!ext) {
        return false;
    }
    return (strcmp(ext, ".yml") == 0 || strcmp(ext, ".yaml") == 0);
}

bool cbm_is_cloudbuild_file(const char *name) {
    if (!name) {
        return false;
    }
    char lower[CBM_SZ_256];
    to_lower(name, lower, sizeof(lower));

    if (strncmp(lower, "cloudbuild", SLEN("cloudbuild")) != 0) {
        return false;
    }
    const char *ext = strrchr(lower, '.');
    if (!ext) {
        return false;
    }
    return (strcmp(ext, ".yml") == 0 || strcmp(ext, ".yaml") == 0);
}

bool cbm_is_env_file(const char *name) {
    if (!name) {
        return false;
    }
    char lower[CBM_SZ_256];
    to_lower(name, lower, sizeof(lower));

    if (strcmp(lower, ".env") == 0) {
        return true;
    }
    if (strncmp(lower, ".env.", SLEN(".env.")) == 0) {
        return true;
    }
    size_t len = strlen(lower);
    if (len > IS_ENV_EXT && strcmp(lower + len - IS_ENV_EXT, ".env") == 0) {
        return true;
    }
    return false;
}

bool cbm_is_kustomize_file(const char *name) {
    if (!name) {
        return false;
    }
    char lower[CBM_SZ_256];
    to_lower(name, lower, sizeof(lower));
    if (strcmp(lower, "kustomization.yaml") == 0) {
        return true;
    }
    return strcmp(lower, "kustomization.yml") == 0;
}

bool cbm_is_k8s_manifest(const char *name, const char *content) {
    if (!name || !content || cbm_is_kustomize_file(name)) {
        return false;
    }
    enum { K8S_PEEK_SZ = 4097 };
    char buf[K8S_PEEK_SZ];
    size_t n = strnlen(content, IS_CONTENT_LIMIT);
    memcpy(buf, content, n);
    buf[n] = '\0';
    return ci_strstr(buf, "apiVersion:") != NULL;
}

bool cbm_is_shell_script(const char *name, const char *ext) {
    (void)name;
    if (!ext) {
        return false;
    }
    return (strcmp(ext, ".sh") == 0 || strcmp(ext, ".bash") == 0 || strcmp(ext, ".zsh") == 0);
}

/* ── Secret detection ───────────────────────────────────────────── */

static bool key_is_secret(const char *key) {
    static const char *patterns[] = {
        "secret",          "password",    "passwd",         "token",      "api_key",
        "apikey",          "private_key", "credential",     "auth_token", "access_key",
        "client_secret",   "signing_key", "encryption_key", "ssh_key",    "deploy_key",
        "service_account", "bearer",      "jwt_secret",     NULL};
    for (int i = 0; patterns[i]; i++) {
        if (ci_strstr(key, patterns[i])) {
            return true;
        }
    }
    return false;
}

bool cbm_is_secret_value(const char *value) {
    if (!value || !*value) {
        return false;
    }

    /* -----BEGIN (PEM key) */
    if (ci_strstr(value, "-----BEGIN")) {
        return true;
    }

    /* Substring searches — Go uses regex MatchString which finds anywhere */
    const char *p;

    /* AKIA + 16 alnum (AWS key) */
    p = ci_strstr(value, "AKIA");
    if (p && count_alnum(p + IS_CMD_SKIP) >= IS_TOKEN_MIN_1) {
        return true;
    }

    /* sk- + 20 alnum (API key) */
    p = ci_strstr(value, "sk-");
    if (p && count_alnum(p + IS_RUN_SKIP) >= IS_TOKEN_MIN_2) {
        return true;
    }

    /* ghp_ + 36 alnum (GitHub PAT) */
    p = ci_strstr(value, "ghp_");
    if (p && count_alnum(p + IS_CMD_SKIP) >= GITHUB_PAT_MIN_ALNUM) {
        return true;
    }

    /* glpat- + 20 alnum/dash (GitLab PAT) */
    p = ci_strstr(value, "glpat-");
    if (p && count_alnum_dash(p + IS_EXPOSE_SKIP) >= IS_TOKEN_MIN_2) {
        return true;
    }

    /* xox[bps]- (Slack token) */
    p = ci_strstr(value, "xox");
    if (p && p[IS_RUN_SKIP] != '\0' &&
        (tolower((unsigned char)p[IS_RUN_SKIP]) == 'b' ||
         tolower((unsigned char)p[IS_RUN_SKIP]) == 'p' ||
         tolower((unsigned char)p[IS_RUN_SKIP]) == 's') &&
        p[IS_CMD_SKIP] == '-' && count_alnum_dash(p + IS_USER_SKIP) >= SKIP_ONE) {
        return true;
    }

    return false;
}

bool cbm_is_secret_binding(const char *key, const char *value) {
    if (key && key_is_secret(key)) {
        return true;
    }
    if (value && cbm_is_secret_value(value)) {
        return true;
    }
    return false;
}

/* ── Clean JSON brackets ────────────────────────────────────────── */

/* Strip JSON array brackets, quotes, and normalize whitespace. */
static void clean_json_array_inner(const char *s, size_t len, char *out, size_t out_sz) {
    size_t pos = 0;
    bool in_space = false;
    for (size_t i = SKIP_ONE; i < len - SKIP_ONE && pos < out_sz - SKIP_ONE; i++) {
        char c = s[i];
        if (c == '"') {
            continue;
        }
        if (c == ',') {
            c = ' ';
        }
        if (c == ' ' || c == '\t') {
            if (!in_space && pos > 0) {
                out[pos++] = ' ';
                in_space = true;
            }
        } else {
            out[pos++] = c;
            in_space = false;
        }
    }
    while (pos > 0 && out[pos - SKIP_ONE] == ' ') {
        pos--;
    }
    out[pos] = '\0';
}

void cbm_clean_json_brackets(const char *s, char *out, size_t out_sz) {
    if (!s || !out || out_sz == 0) {
        return;
    }

    size_t len = strlen(s);
    if (len >= PAIR_LEN && s[0] == '[' && s[len - SKIP_ONE] == ']') {
        clean_json_array_inner(s, len, out, out_sz);
    } else {
        snprintf(out, out_sz, "%s", s);
    }
}

/* ── Dockerfile parser ──────────────────────────────────────────── */

static void df_parse_from(const char *line, cbm_dockerfile_result_t *r) {
    if (strncasecmp(line, "FROM", SLEN("FROM")) != 0) {
        return;
    }
    const char *p = line + IS_FROM_SKIP;
    if (*p != ' ' && *p != '\t') {
        return;
    }
    p = skip_ws(p);

    /* Extract image */
    if (r->stage_count >= IS_MAX_STAGES) {
        return;
    }
    int idx = r->stage_count;
    extract_token(p, r->stage_images[idx], sizeof(r->stage_images[idx]));

    /* Advance past image */
    while (*p && *p != ' ' && *p != '\t') {
        p++;
    }
    p = skip_ws(p);

    /* Check for AS <name> */
    r->stage_names[idx][0] = '\0';
    if (strncasecmp(p, "AS", SLEN("AS")) == 0 && (p[PAIR_LEN] == ' ' || p[PAIR_LEN] == '\t')) {
        p = skip_ws(p + IS_AS_SKIP);
        extract_word(p, r->stage_names[idx], sizeof(r->stage_names[idx]));
    }
    r->stage_count++;
}

static void df_parse_expose(const char *line, cbm_dockerfile_result_t *r) {
    if (strncasecmp(line, "EXPOSE", SLEN("EXPOSE")) != 0) {
        return;
    }
    const char *p = line + IS_EXPOSE_SKIP;
    if (*p != ' ' && *p != '\t') {
        return;
    }
    p = skip_ws(p);

    /* Parse space-separated ports */
    while (*p && r->port_count < IS_MAX_PORTS) {
        char port[CBM_SZ_32];
        int n = extract_token(p, port, sizeof(port));
        if (n == 0) {
            break;
        }

        /* Strip protocol suffix (e.g. 8080/tcp) */
        char *slash = strchr(port, '/');
        if (slash) {
            *slash = '\0';
        }

        snprintf(r->exposed_ports[r->port_count], sizeof(r->exposed_ports[0]), "%s", port);
        r->port_count++;

        p += n;
        p = skip_ws(p);
    }
}

static void df_parse_env(const char *line, cbm_dockerfile_result_t *r) {
    if (strncasecmp(line, "ENV", SLEN("ENV")) != 0) {
        return;
    }
    const char *p = line + IS_RUN_SKIP;
    if (*p != ' ' && *p != '\t') {
        return;
    }
    p = skip_ws(p);

    if (r->env_count >= CBM_SZ_64) {
        return;
    }

    /* Extract key */
    char key[CBM_SZ_128];
    int klen = extract_word(p, key, sizeof(key));
    if (klen == 0) {
        return;
    }
    p += klen;

    /* Separator: = or space */
    if (*p != '=' && *p != ' ' && *p != '\t') {
        return;
    }
    p++;
    /* Skip additional whitespace after separator */
    while (*p == ' ' || *p == '\t') {
        p++;
    }

    /* Value is rest of line (trimmed) */
    char value[CBM_SZ_512];
    snprintf(value, sizeof(value), "%s", p);
    rtrim(value);

    /* Filter secrets */
    if (cbm_is_secret_binding(key, value)) {
        return;
    }

    snprintf(r->env_vars[r->env_count].key, sizeof(r->env_vars[0].key), "%s", key);
    snprintf(r->env_vars[r->env_count].value, sizeof(r->env_vars[0].value), "%s", value);
    r->env_count++;
}

static void df_parse_arg(const char *line, cbm_dockerfile_result_t *r) {
    if (strncasecmp(line, "ARG", SLEN("ARG")) != 0) {
        return;
    }
    const char *p = line + IS_RUN_SKIP;
    if (*p != ' ' && *p != '\t') {
        return;
    }
    p = skip_ws(p);

    if (r->build_arg_count >= CBM_SZ_32) {
        return;
    }
    extract_word(p, r->build_args[r->build_arg_count], sizeof(r->build_args[0]));
    if (r->build_args[r->build_arg_count][0]) {
        r->build_arg_count++;
    }
}

/* Parse a Dockerfile CMD/ENTRYPOINT value: strip brackets, trim. */
static void df_parse_json_cmd(const char *line, int prefix_len, char *out, size_t out_sz) {
    const char *p = skip_ws(line + prefix_len);
    char raw[CBM_SZ_512];
    snprintf(raw, sizeof(raw), "%s", p);
    rtrim(raw);
    cbm_clean_json_brackets(raw, out, out_sz);
}

static void df_parse_directives(const char *line, cbm_dockerfile_result_t *r) {
    if (strncasecmp(line, "WORKDIR", SLEN("WORKDIR")) == 0 &&
        (line[IS_EXPORT_SKIP] == ' ' || line[IS_EXPORT_SKIP] == '\t')) {
        const char *p = skip_ws(line + IS_WORKDIR_SKIP);
        snprintf(r->workdir, sizeof(r->workdir), "%s", p);
        rtrim(r->workdir);
        return;
    }
    if (strncasecmp(line, "CMD", SLEN("CMD")) == 0 &&
        (line[IS_RUN_SKIP] == ' ' || line[IS_RUN_SKIP] == '\t')) {
        df_parse_json_cmd(line, IS_CMD_SKIP, r->cmd, sizeof(r->cmd));
        return;
    }
    if (strncasecmp(line, "ENTRYPOINT", SLEN("ENTRYPOINT")) == 0 &&
        (line[IS_ENTRYPOINT_OFF] == ' ' || line[IS_ENTRYPOINT_OFF] == '\t')) {
        df_parse_json_cmd(line, LEN_HEALTHCHECK, r->entrypoint, sizeof(r->entrypoint));
        return;
    }
    if (strncasecmp(line, "USER", SLEN("USER")) == 0 &&
        (line[IS_CMD_SKIP] == ' ' || line[IS_CMD_SKIP] == '\t')) {
        const char *p = skip_ws(line + IS_USER_SKIP);
        extract_word(p, r->user, sizeof(r->user));
        return;
    }
    if (strncasecmp(line, "HEALTHCHECK", LEN_HEALTHCHECK) == 0 &&
        (line[LEN_HEALTHCHECK] == ' ' || line[LEN_HEALTHCHECK] == '\t')) {
        const char *cmd_pos = ci_strstr(line + LEN_HEALTHCHECK, "CMD");
        if (cmd_pos) {
            cmd_pos = skip_ws(cmd_pos + IS_RUN_SKIP);
            snprintf(r->healthcheck, sizeof(r->healthcheck), "%s", cmd_pos);
            rtrim(r->healthcheck);
        }
    }
}

int cbm_parse_dockerfile_source(const char *source, cbm_dockerfile_result_t *out) {
    if (!source || !out) {
        return CBM_NOT_FOUND;
    }
    memset(out, 0, sizeof(*out));

    /* Process line by line */
    const char *p = source;
    char line[CBM_SZ_4K];

    while (*p) {
        /* Extract one line */
        const char *eol = strchr(p, '\n');
        size_t line_len = eol ? (size_t)(eol - p) : strlen(p);
        if (line_len >= sizeof(line)) {
            line_len = sizeof(line) - SKIP_ONE;
        }
        memcpy(line, p, line_len);
        line[line_len] = '\0';
        p = eol ? eol + SKIP_ONE : p + line_len;

        /* Trim */
        char *trimmed = line;
        while (*trimmed == ' ' || *trimmed == '\t') {
            trimmed++;
        }
        rtrim(trimmed);

        /* Skip empty lines and comments */
        if (*trimmed == '\0' || *trimmed == '#') {
            continue;
        }

        df_parse_from(trimmed, out);
        df_parse_expose(trimmed, out);
        df_parse_env(trimmed, out);
        df_parse_arg(trimmed, out);
        df_parse_directives(trimmed, out);
    }

    /* No stages = empty/invalid Dockerfile */
    if (out->stage_count == 0) {
        return CBM_NOT_FOUND;
    }

    /* base_image = last stage's image (use intermediate copy to avoid restrict overlap) */
    {
        char tmp[sizeof(out->base_image)];
        snprintf(tmp, sizeof(tmp), "%s", out->stage_images[out->stage_count - SKIP_ONE]);
        memcpy(out->base_image, tmp, sizeof(out->base_image));
    }

    return 0;
}

/* ── Dotenv parser ──────────────────────────────────────────────── */

/* Strip matching surrounding quotes from a value string in-place. */
static void strip_surrounding_quotes(char *value) {
    size_t vlen = strlen(value);
    if (vlen >= CBM_QUOTE_PAIR && ((value[0] == '"' && value[vlen - SKIP_ONE] == '"') ||
                                   (value[0] == '\'' && value[vlen - SKIP_ONE] == '\''))) {
        memmove(value, value + CBM_QUOTE_OFFSET, vlen - CBM_QUOTE_PAIR);
        value[vlen - IS_QUOTE_TRIM] = '\0';
    }
}

/* Parse a single dotenv line: KEY=VALUE. Returns 1 if env var added. */
static int parse_dotenv_line(const char *trimmed, cbm_dotenv_result_t *out) {
    char key[CBM_SZ_128];
    int klen = 0;
    if (isalpha((unsigned char)trimmed[0]) || trimmed[0] == '_') {
        klen = extract_word(trimmed, key, sizeof(key));
    }
    if (klen == 0 || trimmed[klen] != '=') {
        return 0;
    }

    char value[CBM_SZ_512];
    snprintf(value, sizeof(value), "%s", trimmed + klen + SKIP_ONE);
    rtrim(value);
    strip_surrounding_quotes(value);

    if (cbm_is_secret_binding(key, value) || out->env_count >= CBM_SZ_64) {
        return 0;
    }
    snprintf(out->env_vars[out->env_count].key, sizeof(out->env_vars[0].key), "%s", key);
    snprintf(out->env_vars[out->env_count].value, sizeof(out->env_vars[0].value), "%s", value);
    out->env_count++;
    return SKIP_ONE;
}

int cbm_parse_dotenv_source(const char *source, cbm_dotenv_result_t *out) {
    if (!source || !out) {
        return CBM_NOT_FOUND;
    }
    memset(out, 0, sizeof(*out));

    const char *p = source;
    char line[CBM_SZ_4K];

    while (*p) {
        const char *eol = strchr(p, '\n');
        size_t line_len = eol ? (size_t)(eol - p) : strlen(p);
        if (line_len >= sizeof(line)) {
            line_len = sizeof(line) - SKIP_ONE;
        }
        memcpy(line, p, line_len);
        line[line_len] = '\0';
        p = eol ? eol + SKIP_ONE : p + line_len;

        char *trimmed = line;
        while (*trimmed == ' ' || *trimmed == '\t') {
            trimmed++;
        }
        rtrim(trimmed);

        if (*trimmed == '\0' || *trimmed == '#') {
            continue;
        }

        parse_dotenv_line(trimmed, out);
    }

    return (out->env_count > 0) ? 0 : IS_NOT_FOUND;
}

/* ── Shell script parser ────────────────────────────────────────── */

static void shell_parse_export(const char *line, cbm_shell_result_t *r) {
    /* export VAR=VALUE */
    if (strncmp(line, "export", SLEN("export")) != 0 ||
        (line[IS_EXPOSE_SKIP] != ' ' && line[IS_EXPOSE_SKIP] != '\t')) {
        return;
    }
    const char *p = skip_ws(line + IS_EXPORT_SKIP);

    char key[CBM_SZ_128];
    int klen = extract_word(p, key, sizeof(key));
    if (klen == 0 || p[klen] != '=') {
        return;
    }

    const char *val_start = p + klen + SKIP_ONE;
    char value[CBM_SZ_512];
    snprintf(value, sizeof(value), "%s", val_start);
    rtrim(value);

    /* Strip surrounding quotes */
    size_t vlen = strlen(value);
    if (vlen >= CBM_QUOTE_PAIR && ((value[0] == '"' && value[vlen - SKIP_ONE] == '"') ||
                                   (value[0] == '\'' && value[vlen - SKIP_ONE] == '\''))) {
        memmove(value, value + CBM_QUOTE_OFFSET, vlen - CBM_QUOTE_PAIR);
        value[vlen - IS_QUOTE_TRIM] = '\0';
    }

    if (cbm_is_secret_binding(key, value)) {
        return;
    }
    if (r->env_count >= CBM_SZ_64) {
        return;
    }
    snprintf(r->env_vars[r->env_count].key, sizeof(r->env_vars[0].key), "%s", key);
    snprintf(r->env_vars[r->env_count].value, sizeof(r->env_vars[0].value), "%s", value);
    r->env_count++;
}

static void shell_parse_plain_var(const char *line, cbm_shell_result_t *r) {
    /* VAR=VALUE (only if no spaces in line — avoids matching commands) */
    if (strchr(line, ' ') || strchr(line, '\t')) {
        return;
    }

    char key[CBM_SZ_128];
    int klen = extract_word(line, key, sizeof(key));
    if (klen == 0 || line[klen] != '=') {
        return;
    }

    const char *val_start = line + klen + SKIP_ONE;
    char value[CBM_SZ_512];
    snprintf(value, sizeof(value), "%s", val_start);
    rtrim(value);

    /* Strip surrounding quotes */
    size_t vlen = strlen(value);
    if (vlen >= CBM_QUOTE_PAIR && ((value[0] == '"' && value[vlen - SKIP_ONE] == '"') ||
                                   (value[0] == '\'' && value[vlen - SKIP_ONE] == '\''))) {
        memmove(value, value + CBM_QUOTE_OFFSET, vlen - CBM_QUOTE_PAIR);
        value[vlen - IS_QUOTE_TRIM] = '\0';
    }

    if (cbm_is_secret_binding(key, value)) {
        return;
    }
    if (r->env_count >= CBM_SZ_64) {
        return;
    }
    snprintf(r->env_vars[r->env_count].key, sizeof(r->env_vars[0].key), "%s", key);
    snprintf(r->env_vars[r->env_count].value, sizeof(r->env_vars[0].value), "%s", value);
    r->env_count++;
}

static void shell_parse_source(const char *line, cbm_shell_result_t *r) {
    /* source <file> or . <file> */
    const char *p = NULL;
    if (strncmp(line, "source", SLEN("source")) == 0 &&
        (line[IS_EXPOSE_SKIP] == ' ' || line[IS_EXPOSE_SKIP] == '\t')) {
        p = skip_ws(line + IS_EXPORT_SKIP);
    } else if (line[0] == '.' && (line[SKIP_ONE] == ' ' || line[SKIP_ONE] == '\t')) {
        p = skip_ws(line + IS_SOURCE_DOT);
    }
    if (!p) {
        return;
    }

    if (r->source_count >= IS_MAX_SOURCES) {
        return;
    }

    /* Strip surrounding quotes */
    char path[CBM_SZ_256];
    snprintf(path, sizeof(path), "%s", p);
    rtrim(path);
    size_t plen = strlen(path);
    if (plen >= CBM_QUOTE_PAIR && ((path[0] == '"' && path[plen - SKIP_ONE] == '"') ||
                                   (path[0] == '\'' && path[plen - SKIP_ONE] == '\''))) {
        memmove(path, path + CBM_QUOTE_OFFSET, plen - CBM_QUOTE_PAIR);
        path[plen - IS_QUOTE_TRIM] = '\0';
    }

    /* Extract just the path token (no trailing args) */
    char *space = strchr(path, ' ');
    if (space) {
        *space = '\0';
    }

    snprintf(r->sources[r->source_count], sizeof(r->sources[0]), "%s", path);
    r->source_count++;
}

static void shell_parse_docker(const char *line, cbm_shell_result_t *r) {
    /* docker <subcmd> or docker-compose <subcmd> */
    const char *p = NULL;
    const char *tool = NULL;

    if (strncmp(line, "docker-compose", LEN_DOCKER_COMPOSE) == 0 &&
        (line[LEN_DOCKER_COMPOSE] == ' ' || line[LEN_DOCKER_COMPOSE] == '\t')) {
        tool = "docker-compose";
        p = skip_ws(line + LEN_DOCKER_COMPOSE_SKIP);
    } else if (strncmp(line, "docker", SLEN("docker")) == 0 &&
               (line[IS_EXPOSE_SKIP] == ' ' || line[IS_EXPOSE_SKIP] == '\t')) {
        tool = "docker";
        p = skip_ws(line + IS_EXPORT_SKIP);
    }
    if (!tool || !p || !*p) {
        return;
    }

    if (r->docker_cmd_count >= IS_MAX_STAGES) {
        return;
    }

    char subcmd[CBM_SZ_64];
    extract_word(p, subcmd, sizeof(subcmd));
    if (subcmd[0]) {
        snprintf(r->docker_cmds[r->docker_cmd_count], sizeof(r->docker_cmds[0]), "%s %s", tool,
                 subcmd);
        r->docker_cmd_count++;
    }
}

/* Check for shebang on first non-empty line. Returns true if line was consumed. */
static bool shell_try_shebang(const char *trimmed, cbm_shell_result_t *out, bool *shebang_checked) {
    if (*shebang_checked) {
        return false;
    }
    if (trimmed[0] == '#' && trimmed[SKIP_ONE] == '!') {
        const char *sb = trimmed + PAIR_LEN;
        while (*sb == ' ') {
            sb++;
        }
        snprintf(out->shebang, sizeof(out->shebang), "%s", sb);
        rtrim(out->shebang);
        *shebang_checked = true;
        return true;
    }
    *shebang_checked = true;
    return false;
}

/* Dispatch a single trimmed shell line to the appropriate parser. */
static void shell_dispatch_line(const char *trimmed, cbm_shell_result_t *out) {
    if (strncmp(trimmed, "export", SLEN("export")) == 0) {
        shell_parse_export(trimmed, out);
        return;
    }
    if (strncmp(trimmed, "source", SLEN("source")) == 0 ||
        (trimmed[0] == '.' && trimmed[SKIP_ONE] == ' ')) {
        shell_parse_source(trimmed, out);
        return;
    }
    if (strncmp(trimmed, "docker", SLEN("docker")) == 0) {
        shell_parse_docker(trimmed, out);
    }
    shell_parse_plain_var(trimmed, out);
}

int cbm_parse_shell_source(const char *source, cbm_shell_result_t *out) {
    if (!source || !out) {
        return CBM_NOT_FOUND;
    }
    memset(out, 0, sizeof(*out));

    const char *p = source;
    char line[CBM_SZ_4K];
    bool shebang_checked = false;

    while (*p) {
        const char *eol = strchr(p, '\n');
        size_t line_len = eol ? (size_t)(eol - p) : strlen(p);
        if (line_len >= sizeof(line)) {
            line_len = sizeof(line) - SKIP_ONE;
        }
        memcpy(line, p, line_len);
        line[line_len] = '\0';
        p = eol ? eol + SKIP_ONE : p + line_len;

        char *trimmed = line;
        while (*trimmed == ' ' || *trimmed == '\t') {
            trimmed++;
        }
        rtrim(trimmed);

        if (*trimmed == '\0') {
            continue;
        }

        if (shell_try_shebang(trimmed, out, &shebang_checked)) {
            continue;
        }

        if (*trimmed == '#') {
            continue;
        }

        shell_dispatch_line(trimmed, out);
    }

    bool has_content = out->shebang[0] != '\0' || out->env_count > 0 || out->source_count > 0 ||
                       out->docker_cmd_count > 0;
    return has_content ? 0 : IS_NOT_FOUND;
}

/* ── Terraform parser ───────────────────────────────────────────── */

typedef enum {
    TF_BLOCK_NONE = 0,
    TF_BLOCK_VARIABLE,
    TF_BLOCK_MODULE,
    TF_BLOCK_TERRAFORM
} tf_block_kind_t;

/* Extract a double-quoted string value after = sign.
 * Handles: key = "value" and key = value */
static void tf_extract_quoted(const char *line, const char *prefix, char *out, size_t out_sz) {
    const char *p = skip_ws(line);
    size_t plen = strlen(prefix);
    if (strncmp(p, prefix, plen) != 0) {
        return;
    }
    p = skip_ws(p + plen);
    if (*p != '=') {
        return;
    }
    p = skip_ws(p + SKIP_ONE);

    /* Strip optional quotes */
    if (*p == '"') {
        p++;
        const char *end = strchr(p, '"');
        size_t vlen = end ? (size_t)(end - p) : strlen(p);
        if (vlen >= out_sz) {
            vlen = out_sz - SKIP_ONE;
        }
        memcpy(out, p, vlen);
        out[vlen] = '\0';
    } else {
        extract_token(p, out, out_sz);
    }
}

/* Count occurrences of a character in a string */
static int count_char(const char *s, char c) {
    int n = 0;
    for (; *s; s++) {
        if (*s == c) {
            n++;
        }
    }
    return n;
}

/* Extract double-quoted identifier after keyword.
 * Matches: keyword "identifier" → sets out to "identifier"
 * Returns number of chars consumed from p, or 0 if no match. */
static int tf_extract_ident(const char *p, const char *keyword, char *out, size_t out_sz) {
    size_t klen = strlen(keyword);
    if (strncmp(p, keyword, klen) != 0) {
        return 0;
    }
    const char *after = skip_ws(p + klen);
    if (*after != '"') {
        return 0;
    }
    after++;
    const char *end = strchr(after, '"');
    if (!end) {
        return 0;
    }
    size_t ilen = (size_t)(end - after);
    if (ilen >= out_sz) {
        ilen = out_sz - SKIP_ONE;
    }
    memcpy(out, after, ilen);
    out[ilen] = '\0';
    return (int)(end - p + SKIP_ONE);
}

/* Parse a second quoted string after the first ident (for resource/data blocks). */
static void tf_extract_second_quoted(const char *trimmed, int consumed, char *out, size_t outsz) {
    const char *rest = skip_ws(trimmed + consumed);
    out[0] = '\0';
    if (*rest == '"') {
        rest++;
        const char *end = strchr(rest, '"');
        if (end) {
            size_t len = (size_t)(end - rest);
            if (len >= outsz) {
                len = outsz - SKIP_ONE;
            }
            memcpy(out, rest, len);
            out[len] = '\0';
        }
    }
}

/* Try to parse a simple "keyword ident" block (variable, output, provider). */
static bool tf_try_simple_block(const char *trimmed, const char *keyword, char *names, int name_sz,
                                int *count, int max_count) {
    char ident[CBM_SZ_128];
    int consumed = tf_extract_ident(trimmed, keyword, ident, sizeof(ident));
    if (consumed <= 0 || *count >= max_count) {
        return false;
    }
    snprintf(names + ((size_t)(*count) * (size_t)name_sz), name_sz, "%s", ident);
    (*count)++;
    return true;
}

/* Try to match terraform/locals keyword blocks. */
static bool tf_try_keyword_block(const char *trimmed, tf_block_kind_t *cur_block,
                                 cbm_terraform_result_t *out) {
    if (strncmp(trimmed, "terraform", SLEN("terraform")) == 0 && strstr(trimmed, "{")) {
        *cur_block = TF_BLOCK_TERRAFORM;
        return true;
    }
    if (strncmp(trimmed, "locals", SLEN("locals")) == 0 && strstr(trimmed, "{")) {
        out->has_locals = true;
        return true;
    }
    return false;
}

/* Detect a top-level block header. Returns true if line was consumed as a header. */
static bool tf_detect_block_header(const char *trimmed, cbm_terraform_result_t *out,
                                   tf_block_kind_t *cur_block, int *cur_var_idx, int *cur_mod_idx) {
    char ident1[CBM_SZ_128];

    /* resource "type" "name" */
    {
        int rc = tf_extract_ident(trimmed, "resource", ident1, sizeof(ident1));
        if (rc > 0 && out->resource_count < CBM_SZ_32) {
            char ident2[CBM_SZ_128];
            tf_extract_second_quoted(trimmed, rc, ident2, sizeof(ident2));
            snprintf(out->resources[out->resource_count].type, sizeof(out->resources[0].type), "%s",
                     ident1);
            snprintf(out->resources[out->resource_count].name, sizeof(out->resources[0].name), "%s",
                     ident2);
            out->resource_count++;
            return true;
        }
    }

    /* variable "name" */
    int consumed = tf_extract_ident(trimmed, "variable", ident1, sizeof(ident1));
    if (consumed > 0 && out->variable_count < CBM_SZ_32) {
        *cur_block = TF_BLOCK_VARIABLE;
        *cur_var_idx = out->variable_count;
        snprintf(out->variables[*cur_var_idx].name, sizeof(out->variables[0].name), "%s", ident1);
        out->variable_count++;
        return true;
    }

    /* output "name" */
    if (tf_try_simple_block(trimmed, "output", out->outputs[0], (int)sizeof(out->outputs[0]),
                            &out->output_count, CBM_SZ_32)) {
        return true;
    }

    /* provider "name" */
    if (tf_try_simple_block(trimmed, "provider", out->providers[0], (int)sizeof(out->providers[0]),
                            &out->provider_count, IS_MAX_STAGES)) {
        return true;
    }

    /* module "name" */
    consumed = tf_extract_ident(trimmed, "module", ident1, sizeof(ident1));
    if (consumed > 0 && out->module_count < IS_MAX_STAGES) {
        *cur_block = TF_BLOCK_MODULE;
        *cur_mod_idx = out->module_count;
        snprintf(out->modules[*cur_mod_idx].tf_name, sizeof(out->modules[0].tf_name), "%s", ident1);
        out->module_count++;
        return true;
    }

    /* data "type" "name" */
    {
        int dc = tf_extract_ident(trimmed, "data", ident1, sizeof(ident1));
        if (dc > 0 && out->data_source_count < IS_MAX_STAGES) {
            char ident2[CBM_SZ_128];
            tf_extract_second_quoted(trimmed, dc, ident2, sizeof(ident2));
            snprintf(out->data_sources[out->data_source_count].type,
                     sizeof(out->data_sources[0].type), "%s", ident1);
            snprintf(out->data_sources[out->data_source_count].name,
                     sizeof(out->data_sources[0].name), "%s", ident2);
            out->data_source_count++;
            return true;
        }
    }

    return tf_try_keyword_block(trimmed, cur_block, out);
}

/* Extract attributes from a line inside a variable block. */
static void tf_parse_variable_attrs(const char *trimmed, cbm_tf_variable_t *v) {
    char def_val[CBM_SZ_256] = {0};
    tf_extract_quoted(trimmed, "default", def_val, sizeof(def_val));
    if (def_val[0] && !cbm_is_secret_binding(v->name, def_val)) {
        snprintf(v->default_val, sizeof(v->default_val), "%s", def_val);
    }
    char type_val[CBM_SZ_64] = {0};
    tf_extract_quoted(trimmed, "type", type_val, sizeof(type_val));
    if (type_val[0]) {
        snprintf(v->type, sizeof(v->type), "%s", type_val);
    }
    char desc_val[CBM_SZ_256] = {0};
    tf_extract_quoted(trimmed, "description", desc_val, sizeof(desc_val));
    if (desc_val[0]) {
        snprintf(v->description, sizeof(v->description), "%s", desc_val);
    }
}

/* Extract attributes inside a block (variable/module/terraform). */
static void tf_parse_block_attrs(const char *trimmed, cbm_terraform_result_t *out,
                                 tf_block_kind_t cur_block, int cur_var_idx, int cur_mod_idx) {
    switch (cur_block) {
    case TF_BLOCK_VARIABLE:
        if (cur_var_idx >= 0) {
            tf_parse_variable_attrs(trimmed, &out->variables[cur_var_idx]);
        }
        break;
    case TF_BLOCK_MODULE:
        if (cur_mod_idx >= 0) {
            char src_val[CBM_SZ_256] = {0};
            tf_extract_quoted(trimmed, "source", src_val, sizeof(src_val));
            if (src_val[0]) {
                snprintf(out->modules[cur_mod_idx].source, sizeof(out->modules[0].source), "%s",
                         src_val);
            }
        }
        break;
    case TF_BLOCK_TERRAFORM: {
        const char *bp = skip_ws(trimmed);
        char backend_name[CBM_SZ_128] = {0};
        tf_extract_ident(bp, "backend", backend_name, sizeof(backend_name));
        if (backend_name[0]) {
            snprintf(out->backend, sizeof(out->backend), "%s", backend_name);
        }
        break;
    }
    default:
        break;
    }
}

/* Check if any terraform content was parsed. */
static bool tf_result_has_content(const cbm_terraform_result_t *out) {
    return out->resource_count > 0 || out->variable_count > 0 || out->output_count > 0 ||
           out->provider_count > 0 || out->module_count > 0 || out->data_source_count > 0 ||
           out->backend[0] != '\0' || out->has_locals;
}

/* Read next non-empty, non-comment line. Advances *pp. Returns trimmed or NULL. */
static char *tf_next_line(const char **pp, char *line, size_t line_sz) {
    const char *p = *pp;
    const char *eol = strchr(p, '\n');
    size_t len = eol ? (size_t)(eol - p) : strlen(p);
    if (len >= line_sz) {
        len = line_sz - SKIP_ONE;
    }
    memcpy(line, p, len);
    line[len] = '\0';
    *pp = eol ? eol + SKIP_ONE : p + len;

    char *trimmed = line;
    while (*trimmed == ' ' || *trimmed == '\t') {
        trimmed++;
    }
    rtrim(trimmed);
    if (*trimmed == '\0' || *trimmed == '#' || (trimmed[0] == '/' && trimmed[SKIP_ONE] == '/')) {
        return NULL;
    }
    return trimmed;
}

int cbm_parse_terraform_source(const char *source, cbm_terraform_result_t *out) {
    if (!source || !out) {
        return CBM_NOT_FOUND;
    }
    memset(out, 0, sizeof(*out));

    const char *p = source;
    char line[CBM_SZ_4K];
    int brace_depth = 0;
    tf_block_kind_t cur_block = TF_BLOCK_NONE;
    int cur_var_idx = IS_NOT_FOUND;
    int cur_mod_idx = IS_NOT_FOUND;

    while (*p) {
        char *trimmed = tf_next_line(&p, line, sizeof(line));
        if (!trimmed) {
            continue;
        }

        brace_depth += count_char(trimmed, '{') - count_char(trimmed, '}');

        if (brace_depth <= SKIP_ONE && cur_block == TF_BLOCK_NONE) {
            if (tf_detect_block_header(trimmed, out, &cur_block, &cur_var_idx, &cur_mod_idx)) {
                continue;
            }
        }

        if (cur_block != TF_BLOCK_NONE && brace_depth >= SKIP_ONE && brace_depth <= PAIR_LEN) {
            tf_parse_block_attrs(trimmed, out, cur_block, cur_var_idx, cur_mod_idx);
        }

        if (brace_depth == 0 && cur_block != TF_BLOCK_NONE) {
            cur_block = TF_BLOCK_NONE;
            cur_var_idx = IS_NOT_FOUND;
            cur_mod_idx = IS_NOT_FOUND;
        }
    }

    return tf_result_has_content(out) ? 0 : IS_NOT_FOUND;
}

/* ── Infra QN helper ────────────────────────────────────────────── */

char *cbm_infra_qn(const char *project_name, const char *rel_path, const char *infra_type,
                   const char *service_name) {
    char *base = cbm_pipeline_fqn_compute(project_name, rel_path, "");
    if (!base) {
        return NULL;
    }

    char result[CBM_SZ_1K];
    if (service_name && service_name[0] && infra_type &&
        strcmp(infra_type, "compose-service") == 0) {
        snprintf(result, sizeof(result), "%s::%s", base, service_name);
    } else {
        snprintf(result, sizeof(result), "%s.__infra__", base);
    }

    free(base);
    return strdup(result);
}
