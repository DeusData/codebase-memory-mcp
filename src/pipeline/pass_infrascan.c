/*
 * pass_infrascan.c — Infrastructure file detection and parsing helpers.
 *
 * Pure helper functions for detecting infrastructure file types,
 * secret filtering, and parsing Dockerfiles, .env files, shell scripts,
 * and Terraform files.
 */
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
        len = dst_sz - 1;
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
        // NOLINTNEXTLINE(misc-include-cleaner) — strncasecmp provided by standard header
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
    while ((isalnum((unsigned char)p[n]) || p[n] == '_') && (size_t)n < buf_sz - 1) {
        buf[n] = p[n];
        n++;
    }
    buf[n] = '\0';
    return n;
}

/* Extract a non-space token into buf. Returns length. */
static int extract_token(const char *p, char *buf, size_t buf_sz) {
    int n = 0;
    while (p[n] && p[n] != ' ' && p[n] != '\t' && (size_t)n < buf_sz - 1) {
        buf[n] = p[n];
        n++;
    }
    buf[n] = '\0';
    return n;
}

/* Trim trailing whitespace in-place */
static void rtrim(char *s) {
    size_t len = strlen(s);
    while (len > 0 &&
           (s[len - 1] == ' ' || s[len - 1] == '\t' || s[len - 1] == '\r' || s[len - 1] == '\n')) {
        s[--len] = '\0';
    }
}

/* ── File identification ────────────────────────────────────────── */

bool cbm_is_dockerfile(const char *name) {
    if (!name) {
        return false;
    }
    char lower[256];
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
    char lower[256];
    to_lower(name, lower, sizeof(lower));

    // NOLINTNEXTLINE(readability-implicit-bool-conversion)
    bool prefix_match = (strncmp(lower, "docker-compose", LEN_DOCKER_COMPOSE) == 0) ||
                        (strcmp(lower, "compose.yml") == 0) || (strcmp(lower, "compose.yaml") == 0);
    if (!prefix_match) {
        return false;
    }

    const char *ext = strrchr(lower, '.');
    if (!ext) {
        return false;
    }
    // NOLINTNEXTLINE(readability-implicit-bool-conversion)
    return (strcmp(ext, ".yml") == 0 || strcmp(ext, ".yaml") == 0);
}

bool cbm_is_cloudbuild_file(const char *name) {
    if (!name) {
        return false;
    }
    char lower[256];
    to_lower(name, lower, sizeof(lower));

    if (strncmp(lower, "cloudbuild", 10) != 0) {
        return false;
    }
    const char *ext = strrchr(lower, '.');
    if (!ext) {
        return false;
    }
    // NOLINTNEXTLINE(readability-implicit-bool-conversion)
    return (strcmp(ext, ".yml") == 0 || strcmp(ext, ".yaml") == 0);
}

bool cbm_is_env_file(const char *name) {
    if (!name) {
        return false;
    }
    char lower[256];
    to_lower(name, lower, sizeof(lower));

    if (strcmp(lower, ".env") == 0) {
        return true;
    }
    if (strncmp(lower, ".env.", 5) == 0) {
        return true;
    }
    size_t len = strlen(lower);
    if (len > 4 && strcmp(lower + len - 4, ".env") == 0) {
        return true;
    }
    return false;
}

bool cbm_is_kustomize_file(const char *name) {
    if (!name) {
        return false;
    }
    char lower[256];
    to_lower(name, lower, sizeof(lower));
    if (strcmp(lower, "kustomization.yaml") == 0) {
        return true;
    }
    return strcmp(lower, "kustomization.yml") == 0;
}

// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
bool cbm_is_k8s_manifest(const char *name, const char *content) {
    if (!name || !content || cbm_is_kustomize_file(name)) {
        return false;
    }
    enum { K8S_PEEK_SZ = 4097 };
    char buf[K8S_PEEK_SZ];
    size_t n = strnlen(content, 4096);
    memcpy(buf, content, n);
    buf[n] = '\0';
    return ci_strstr(buf, "apiVersion:") != NULL;
}

// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
bool cbm_is_shell_script(const char *name, const char *ext) {
    (void)name;
    if (!ext) {
        return false;
    }
    // NOLINTNEXTLINE(readability-implicit-bool-conversion)
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
    // NOLINTNEXTLINE(bugprone-assignment-in-if-condition)
    if ((p = ci_strstr(value, "AKIA")) && count_alnum(p + 4) >= 16) {
        return true;
    }

    /* sk- + 20 alnum (API key) */
    // NOLINTNEXTLINE(bugprone-assignment-in-if-condition)
    if ((p = ci_strstr(value, "sk-")) && count_alnum(p + 3) >= 20) {
        return true;
    }

    /* ghp_ + 36 alnum (GitHub PAT) */
    // NOLINTNEXTLINE(bugprone-assignment-in-if-condition)
    if ((p = ci_strstr(value, "ghp_")) && count_alnum(p + 4) >= GITHUB_PAT_MIN_ALNUM) {
        return true;
    }

    /* glpat- + 20 alnum/dash (GitLab PAT) */
    // NOLINTNEXTLINE(bugprone-assignment-in-if-condition)
    if ((p = ci_strstr(value, "glpat-")) && count_alnum_dash(p + 6) >= 20) {
        return true;
    }

    /* xox[bps]- (Slack token) */
    // NOLINTNEXTLINE(bugprone-assignment-in-if-condition)
    if ((p = ci_strstr(value, "xox")) && p[3] &&
        (tolower((unsigned char)p[3]) == 'b' || tolower((unsigned char)p[3]) == 'p' ||
         tolower((unsigned char)p[3]) == 's') &&
        p[4] == '-' && count_alnum_dash(p + 5) >= 1) {
        return true;
    }

    return false;
}

// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
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

void cbm_clean_json_brackets(const char *s, char *out, size_t out_sz) {
    if (!s || !out || out_sz == 0) {
        return;
    }

    size_t len = strlen(s);
    if (len >= 2 && s[0] == '[' && s[len - 1] == ']') {
        size_t pos = 0;
        bool in_space = false;
        for (size_t i = 1; i < len - 1 && pos < out_sz - 1; i++) {
            char c = s[i];
            if (c == '"') {
                continue; /* strip quotes */
            }
            if (c == ',') {
                c = ' '; /* commas → spaces */
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
        /* Trim trailing space */
        while (pos > 0 && out[pos - 1] == ' ') {
            pos--;
        }
        out[pos] = '\0';
    } else {
        snprintf(out, out_sz, "%s", s);
    }
}

/* ── Dockerfile parser ──────────────────────────────────────────── */

static void df_parse_from(const char *line, cbm_dockerfile_result_t *r) {
    if (strncasecmp(line, "FROM", 4) != 0) {
        return;
    }
    const char *p = line + 4;
    if (*p != ' ' && *p != '\t') {
        return;
    }
    p = skip_ws(p);

    /* Extract image */
    if (r->stage_count >= 16) {
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
    if (strncasecmp(p, "AS", 2) == 0 && (p[2] == ' ' || p[2] == '\t')) {
        p = skip_ws(p + 3);
        extract_word(p, r->stage_names[idx], sizeof(r->stage_names[idx]));
    }
    r->stage_count++;
}

static void df_parse_expose(const char *line, cbm_dockerfile_result_t *r) {
    if (strncasecmp(line, "EXPOSE", 6) != 0) {
        return;
    }
    const char *p = line + 6;
    if (*p != ' ' && *p != '\t') {
        return;
    }
    p = skip_ws(p);

    /* Parse space-separated ports */
    while (*p && r->port_count < 16) {
        char port[32];
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
    if (strncasecmp(line, "ENV", 3) != 0) {
        return;
    }
    const char *p = line + 3;
    if (*p != ' ' && *p != '\t') {
        return;
    }
    p = skip_ws(p);

    if (r->env_count >= 64) {
        return;
    }

    /* Extract key */
    char key[128];
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
    char value[512];
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
    if (strncasecmp(line, "ARG", 3) != 0) {
        return;
    }
    const char *p = line + 3;
    if (*p != ' ' && *p != '\t') {
        return;
    }
    p = skip_ws(p);

    if (r->build_arg_count >= 32) {
        return;
    }
    extract_word(p, r->build_args[r->build_arg_count], sizeof(r->build_args[0]));
    if (r->build_args[r->build_arg_count][0]) {
        r->build_arg_count++;
    }
}

static void df_parse_directives(const char *line, cbm_dockerfile_result_t *r) {
    /* WORKDIR */
    if (strncasecmp(line, "WORKDIR", 7) == 0 && (line[7] == ' ' || line[7] == '\t')) {
        const char *p = skip_ws(line + 8);
        snprintf(r->workdir, sizeof(r->workdir), "%s", p);
        rtrim(r->workdir);
        return;
    }
    /* CMD */
    if (strncasecmp(line, "CMD", 3) == 0 && (line[3] == ' ' || line[3] == '\t')) {
        const char *p = skip_ws(line + 4);
        char raw[512];
        snprintf(raw, sizeof(raw), "%s", p);
        rtrim(raw);
        cbm_clean_json_brackets(raw, r->cmd, sizeof(r->cmd));
        return;
    }
    /* ENTRYPOINT */
    if (strncasecmp(line, "ENTRYPOINT", 10) == 0 && (line[10] == ' ' || line[10] == '\t')) {
        const char *p = skip_ws(line + LEN_HEALTHCHECK);
        char raw[512];
        snprintf(raw, sizeof(raw), "%s", p);
        rtrim(raw);
        cbm_clean_json_brackets(raw, r->entrypoint, sizeof(r->entrypoint));
        return;
    }
    /* USER */
    if (strncasecmp(line, "USER", 4) == 0 && (line[4] == ' ' || line[4] == '\t')) {
        const char *p = skip_ws(line + 5);
        extract_word(p, r->user, sizeof(r->user));
        return;
    }
    /* HEALTHCHECK ... CMD <command> */
    if (strncasecmp(line, "HEALTHCHECK", LEN_HEALTHCHECK) == 0 &&
        (line[LEN_HEALTHCHECK] == ' ' || line[LEN_HEALTHCHECK] == '\t')) {
        const char *cmd_pos = ci_strstr(line + LEN_HEALTHCHECK, "CMD");
        if (cmd_pos) {
            cmd_pos += 3;
            cmd_pos = skip_ws(cmd_pos);
            snprintf(r->healthcheck, sizeof(r->healthcheck), "%s", cmd_pos);
            rtrim(r->healthcheck);
        }
    }
}

int cbm_parse_dockerfile_source(const char *source, cbm_dockerfile_result_t *out) {
    if (!source || !out) {
        return -1;
    }
    memset(out, 0, sizeof(*out));

    /* Process line by line */
    const char *p = source;
    char line[4096];

    while (*p) {
        /* Extract one line */
        const char *eol = strchr(p, '\n');
        size_t line_len = eol ? (size_t)(eol - p) : strlen(p);
        if (line_len >= sizeof(line)) {
            line_len = sizeof(line) - 1;
        }
        memcpy(line, p, line_len);
        line[line_len] = '\0';
        p = eol ? eol + 1 : p + line_len;

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
        return -1;
    }

    /* base_image = last stage's image (use intermediate copy to avoid restrict overlap) */
    {
        char tmp[sizeof(out->base_image)];
        snprintf(tmp, sizeof(tmp), "%s", out->stage_images[out->stage_count - 1]);
        memcpy(out->base_image, tmp, sizeof(out->base_image));
    }

    return 0;
}

/* ── Dotenv parser ──────────────────────────────────────────────── */

int cbm_parse_dotenv_source(const char *source, cbm_dotenv_result_t *out) {
    if (!source || !out) {
        return -1;
    }
    memset(out, 0, sizeof(*out));

    const char *p = source;
    char line[4096];

    while (*p) {
        const char *eol = strchr(p, '\n');
        size_t line_len = eol ? (size_t)(eol - p) : strlen(p);
        if (line_len >= sizeof(line)) {
            line_len = sizeof(line) - 1;
        }
        memcpy(line, p, line_len);
        line[line_len] = '\0';
        p = eol ? eol + 1 : p + line_len;

        char *trimmed = line;
        while (*trimmed == ' ' || *trimmed == '\t') {
            trimmed++;
        }
        rtrim(trimmed);

        if (*trimmed == '\0' || *trimmed == '#') {
            continue;
        }

        /* Match: KEY=VALUE where KEY starts with [A-Za-z_] */
        char key[128];
        int klen = 0;
        if (isalpha((unsigned char)trimmed[0]) || trimmed[0] == '_') {
            klen = extract_word(trimmed, key, sizeof(key));
        }
        if (klen == 0 || trimmed[klen] != '=') {
            continue;
        }

        /* Value: strip surrounding quotes */
        const char *val_start = trimmed + klen + 1;
        char value[512];
        snprintf(value, sizeof(value), "%s", val_start);
        rtrim(value);

        /* Strip matching quotes */
        size_t vlen = strlen(value);
        if (vlen >= 2 && ((value[0] == '"' && value[vlen - 1] == '"') ||
                          (value[0] == '\'' && value[vlen - 1] == '\''))) {
            memmove(value, value + 1, vlen - 2);
            value[vlen - 2] = '\0';
        }

        /* Filter secrets */
        if (cbm_is_secret_binding(key, value)) {
            continue;
        }

        if (out->env_count >= 64) {
            continue;
        }
        snprintf(out->env_vars[out->env_count].key, sizeof(out->env_vars[0].key), "%s", key);
        snprintf(out->env_vars[out->env_count].value, sizeof(out->env_vars[0].value), "%s", value);
        out->env_count++;
    }

    return (out->env_count > 0) ? 0 : -1;
}

/* ── Shell script parser ────────────────────────────────────────── */

static void shell_parse_export(const char *line, cbm_shell_result_t *r) {
    /* export VAR=VALUE */
    if (strncmp(line, "export", 6) != 0 || (line[6] != ' ' && line[6] != '\t')) {
        return;
    }
    const char *p = skip_ws(line + 7);

    char key[128];
    int klen = extract_word(p, key, sizeof(key));
    if (klen == 0 || p[klen] != '=') {
        return;
    }

    const char *val_start = p + klen + 1;
    char value[512];
    snprintf(value, sizeof(value), "%s", val_start);
    rtrim(value);

    /* Strip surrounding quotes */
    size_t vlen = strlen(value);
    if (vlen >= 2 && ((value[0] == '"' && value[vlen - 1] == '"') ||
                      (value[0] == '\'' && value[vlen - 1] == '\''))) {
        memmove(value, value + 1, vlen - 2);
        value[vlen - 2] = '\0';
    }

    if (cbm_is_secret_binding(key, value)) {
        return;
    }
    if (r->env_count >= 64) {
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

    char key[128];
    int klen = extract_word(line, key, sizeof(key));
    if (klen == 0 || line[klen] != '=') {
        return;
    }

    const char *val_start = line + klen + 1;
    char value[512];
    snprintf(value, sizeof(value), "%s", val_start);
    rtrim(value);

    /* Strip surrounding quotes */
    size_t vlen = strlen(value);
    if (vlen >= 2 && ((value[0] == '"' && value[vlen - 1] == '"') ||
                      (value[0] == '\'' && value[vlen - 1] == '\''))) {
        memmove(value, value + 1, vlen - 2);
        value[vlen - 2] = '\0';
    }

    if (cbm_is_secret_binding(key, value)) {
        return;
    }
    if (r->env_count >= 64) {
        return;
    }
    snprintf(r->env_vars[r->env_count].key, sizeof(r->env_vars[0].key), "%s", key);
    snprintf(r->env_vars[r->env_count].value, sizeof(r->env_vars[0].value), "%s", value);
    r->env_count++;
}

static void shell_parse_source(const char *line, cbm_shell_result_t *r) {
    /* source <file> or . <file> */
    const char *p = NULL;
    if (strncmp(line, "source", 6) == 0 && (line[6] == ' ' || line[6] == '\t')) {
        p = skip_ws(line + 7);
    } else if (line[0] == '.' && (line[1] == ' ' || line[1] == '\t')) {
        p = skip_ws(line + 2);
    }
    if (!p) {
        return;
    }

    if (r->source_count >= 16) {
        return;
    }

    /* Strip surrounding quotes */
    char path[256];
    snprintf(path, sizeof(path), "%s", p);
    rtrim(path);
    size_t plen = strlen(path);
    if (plen >= 2 && ((path[0] == '"' && path[plen - 1] == '"') ||
                      (path[0] == '\'' && path[plen - 1] == '\''))) {
        memmove(path, path + 1, plen - 2);
        path[plen - 2] = '\0';
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
    } else if (strncmp(line, "docker", 6) == 0 && (line[6] == ' ' || line[6] == '\t')) {
        tool = "docker";
        p = skip_ws(line + 7);
    }
    if (!tool || !p || !*p) {
        return;
    }

    if (r->docker_cmd_count >= 16) {
        return;
    }

    char subcmd[64];
    extract_word(p, subcmd, sizeof(subcmd));
    if (subcmd[0]) {
        snprintf(r->docker_cmds[r->docker_cmd_count], sizeof(r->docker_cmds[0]), "%s %s", tool,
                 subcmd);
        r->docker_cmd_count++;
    }
}

int cbm_parse_shell_source(const char *source, cbm_shell_result_t *out) {
    if (!source || !out) {
        return -1;
    }
    memset(out, 0, sizeof(*out));

    const char *p = source;
    char line[4096];
    bool shebang_checked = false;

    while (*p) {
        const char *eol = strchr(p, '\n');
        size_t line_len = eol ? (size_t)(eol - p) : strlen(p);
        if (line_len >= sizeof(line)) {
            line_len = sizeof(line) - 1;
        }
        memcpy(line, p, line_len);
        line[line_len] = '\0';
        p = eol ? eol + 1 : p + line_len;

        char *trimmed = line;
        while (*trimmed == ' ' || *trimmed == '\t') {
            trimmed++;
        }
        rtrim(trimmed);

        if (*trimmed == '\0') {
            continue;
        }

        /* Check shebang (first non-empty line starting with #!) */
        if (!shebang_checked && out->shebang[0] == '\0') {
            if (trimmed[0] == '#' && trimmed[1] == '!') {
                const char *sb = trimmed + 2;
                while (*sb == ' ') {
                    sb++;
                }
                snprintf(out->shebang, sizeof(out->shebang), "%s", sb);
                rtrim(out->shebang);
                shebang_checked = true;
                continue;
            }
            shebang_checked = true;
        }

        /* Skip comments */
        if (*trimmed == '#') {
            continue;
        }

        /* Try export first */
        if (strncmp(trimmed, "export", 6) == 0) {
            shell_parse_export(trimmed, out);
            continue;
        }

        /* Try source/dot */
        if (strncmp(trimmed, "source", 6) == 0 || (trimmed[0] == '.' && trimmed[1] == ' ')) {
            shell_parse_source(trimmed, out);
            continue;
        }

        /* Try docker commands */
        if (strncmp(trimmed, "docker", 6) == 0) {
            shell_parse_docker(trimmed, out);
        }

        /* Try plain var (only if no spaces) */
        shell_parse_plain_var(trimmed, out);
    }

    /* Result is non-empty if we have more than just infra_type.
     * In Go: len(props) > 1, where props always has infra_type.
     * Here: check if we have shebang OR env_vars OR sources OR docker_cmds. */
    // NOLINTNEXTLINE(readability-implicit-bool-conversion)
    bool has_content = out->shebang[0] != '\0' || out->env_count > 0 || out->source_count > 0 ||
                       out->docker_cmd_count > 0;
    // NOLINTNEXTLINE(readability-implicit-bool-conversion)
    return has_content ? 0 : -1;
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
// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
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
    p = skip_ws(p + 1);

    /* Strip optional quotes */
    if (*p == '"') {
        p++;
        const char *end = strchr(p, '"');
        size_t vlen = end ? (size_t)(end - p) : strlen(p);
        if (vlen >= out_sz) {
            vlen = out_sz - 1;
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
        ilen = out_sz - 1;
    }
    memcpy(out, after, ilen);
    out[ilen] = '\0';
    return (int)(end - p + 1);
}

int cbm_parse_terraform_source(const char *source, cbm_terraform_result_t *out) {
    if (!source || !out) {
        return -1;
    }
    memset(out, 0, sizeof(*out));

    const char *p = source;
    char line[4096];
    int brace_depth = 0;
    tf_block_kind_t cur_block = TF_BLOCK_NONE;

    /* Current block data indices (for variable/module accumulation) */
    int cur_var_idx = -1;
    int cur_mod_idx = -1;

    while (*p) {
        const char *eol = strchr(p, '\n');
        size_t line_len = eol ? (size_t)(eol - p) : strlen(p);
        if (line_len >= sizeof(line)) {
            line_len = sizeof(line) - 1;
        }
        memcpy(line, p, line_len);
        line[line_len] = '\0';
        p = eol ? eol + 1 : p + line_len;

        char *trimmed = line;
        while (*trimmed == ' ' || *trimmed == '\t') {
            trimmed++;
        }
        rtrim(trimmed);

        if (*trimmed == '\0' || *trimmed == '#' || (trimmed[0] == '/' && trimmed[1] == '/')) {
            continue;
        }

        /* Track brace depth */
        brace_depth += count_char(trimmed, '{') - count_char(trimmed, '}');

        /* At top level (depth <= 1), detect block headers */
        if (brace_depth <= 1 && cur_block == TF_BLOCK_NONE) {
            char ident1[128];
            char ident2[128];

            /* resource "type" "name" */
            int consumed = tf_extract_ident(trimmed, "resource", ident1, sizeof(ident1));
            if (consumed > 0) {
                const char *rest = skip_ws(trimmed + consumed);
                tf_extract_ident(rest, "", ident2, sizeof(ident2));
                /* Actually need to parse second quoted string directly */
                if (*rest == '"') {
                    rest++;
                    const char *end = strchr(rest, '"');
                    if (end) {
                        size_t len = (size_t)(end - rest);
                        if (len >= sizeof(ident2)) {
                            len = sizeof(ident2) - 1;
                        }
                        memcpy(ident2, rest, len);
                        ident2[len] = '\0';
                    }
                }
                if (out->resource_count < 32) {
                    snprintf(out->resources[out->resource_count].type,
                             sizeof(out->resources[0].type), "%s", ident1);
                    snprintf(out->resources[out->resource_count].name,
                             sizeof(out->resources[0].name), "%s", ident2);
                    out->resource_count++;
                }
                continue;
            }

            /* variable "name" */
            consumed = tf_extract_ident(trimmed, "variable", ident1, sizeof(ident1));
            if (consumed > 0 && out->variable_count < 32) {
                cur_block = TF_BLOCK_VARIABLE;
                cur_var_idx = out->variable_count;
                snprintf(out->variables[cur_var_idx].name, sizeof(out->variables[0].name), "%s",
                         ident1);
                out->variable_count++;
                continue;
            }

            /* output "name" */
            consumed = tf_extract_ident(trimmed, "output", ident1, sizeof(ident1));
            if (consumed > 0 && out->output_count < 32) {
                snprintf(out->outputs[out->output_count], sizeof(out->outputs[0]), "%s", ident1);
                out->output_count++;
                continue;
            }

            /* provider "name" */
            consumed = tf_extract_ident(trimmed, "provider", ident1, sizeof(ident1));
            if (consumed > 0 && out->provider_count < 16) {
                snprintf(out->providers[out->provider_count], sizeof(out->providers[0]), "%s",
                         ident1);
                out->provider_count++;
                continue;
            }

            /* module "name" */
            consumed = tf_extract_ident(trimmed, "module", ident1, sizeof(ident1));
            if (consumed > 0 && out->module_count < 16) {
                cur_block = TF_BLOCK_MODULE;
                cur_mod_idx = out->module_count;
                snprintf(out->modules[cur_mod_idx].tf_name, sizeof(out->modules[0].tf_name), "%s",
                         ident1);
                out->module_count++;
                continue;
            }

            /* data "type" "name" */
            consumed = tf_extract_ident(trimmed, "data", ident1, sizeof(ident1));
            if (consumed > 0) {
                const char *rest = skip_ws(trimmed + consumed);
                ident2[0] = '\0';
                if (*rest == '"') {
                    rest++;
                    const char *end = strchr(rest, '"');
                    if (end) {
                        size_t len = (size_t)(end - rest);
                        if (len >= sizeof(ident2)) {
                            len = sizeof(ident2) - 1;
                        }
                        memcpy(ident2, rest, len);
                        ident2[len] = '\0';
                    }
                }
                if (out->data_source_count < 16) {
                    snprintf(out->data_sources[out->data_source_count].type,
                             sizeof(out->data_sources[0].type), "%s", ident1);
                    snprintf(out->data_sources[out->data_source_count].name,
                             sizeof(out->data_sources[0].name), "%s", ident2);
                    out->data_source_count++;
                }
                continue;
            }

            /* terraform { */
            if (strncmp(trimmed, "terraform", 9) == 0 && strstr(trimmed, "{")) {
                cur_block = TF_BLOCK_TERRAFORM;
                continue;
            }

            /* locals { */
            if (strncmp(trimmed, "locals", 6) == 0 && strstr(trimmed, "{")) {
                out->has_locals = true;
                continue;
            }
        }

        /* Inside a block, extract attributes (depth 1-2) */
        if (cur_block != TF_BLOCK_NONE && brace_depth >= 1 && brace_depth <= 2) {
            switch (cur_block) {
            case TF_BLOCK_VARIABLE:
                if (cur_var_idx >= 0) {
                    cbm_tf_variable_t *v = &out->variables[cur_var_idx];
                    /* default = "value" */
                    char def_val[256] = {0};
                    tf_extract_quoted(trimmed, "default", def_val, sizeof(def_val));
                    if (def_val[0] && !cbm_is_secret_binding(v->name, def_val)) {
                        snprintf(v->default_val, sizeof(v->default_val), "%s", def_val);
                    }
                    /* type = <type> */
                    char type_val[64] = {0};
                    tf_extract_quoted(trimmed, "type", type_val, sizeof(type_val));
                    if (type_val[0]) {
                        snprintf(v->type, sizeof(v->type), "%s", type_val);
                    }
                    /* description = "desc" */
                    char desc_val[256] = {0};
                    tf_extract_quoted(trimmed, "description", desc_val, sizeof(desc_val));
                    if (desc_val[0]) {
                        snprintf(v->description, sizeof(v->description), "%s", desc_val);
                    }
                }
                break;
            case TF_BLOCK_MODULE:
                if (cur_mod_idx >= 0) {
                    char src_val[256] = {0};
                    tf_extract_quoted(trimmed, "source", src_val, sizeof(src_val));
                    if (src_val[0]) {
                        snprintf(out->modules[cur_mod_idx].source, sizeof(out->modules[0].source),
                                 "%s", src_val);
                    }
                }
                break;
            case TF_BLOCK_TERRAFORM: {
                /* backend "name" */
                const char *bp = skip_ws(trimmed);
                char backend_name[128] = {0};
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

        /* Block closed */
        if (brace_depth == 0 && cur_block != TF_BLOCK_NONE) {
            cur_block = TF_BLOCK_NONE;
            cur_var_idx = -1;
            cur_mod_idx = -1;
        }
    }

    /* Check if we extracted anything useful */
    // NOLINTNEXTLINE(readability-implicit-bool-conversion)
    bool has_content = out->resource_count > 0 || out->variable_count > 0 ||
                       out->output_count > 0 || out->provider_count > 0 || out->module_count > 0 ||
                       out->data_source_count > 0 || out->backend[0] != '\0' || out->has_locals;
    // NOLINTNEXTLINE(readability-implicit-bool-conversion)
    return has_content ? 0 : -1;
}

/* ── Infra QN helper ────────────────────────────────────────────── */

// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
char *cbm_infra_qn(const char *project_name, const char *rel_path, const char *infra_type,
                   const char *service_name) {
    char *base = cbm_pipeline_fqn_compute(project_name, rel_path, "");
    if (!base) {
        return NULL;
    }

    char result[1024];
    if (service_name && service_name[0] && infra_type &&
        strcmp(infra_type, "compose-service") == 0) {
        snprintf(result, sizeof(result), "%s::%s", base, service_name);
    } else {
        snprintf(result, sizeof(result), "%s.__infra__", base);
    }

    free(base);
    // NOLINTNEXTLINE(misc-include-cleaner) — strdup provided by standard header
    return strdup(result);
}
