/*
 * pass_infrascan.c — Infrastructure file helpers.
 *
 * Kustomization / K8s manifest detection, secret filtering, the infra QN
 * helper, and the Helm Chart.yaml parser.
 */
#include "foundation/constants.h"

enum {
    IS_CONTENT_LIMIT = 4096,
    IS_TOKEN_MIN_1 = 16,  /* min alnum for AWS key */
    IS_TOKEN_MIN_2 = 20,  /* min alnum for GH classic token */
    SECRET_AKIA_LEN = 4,  /* strlen("AKIA") */
    SECRET_SK_LEN = 3,    /* strlen("sk-") */
    SECRET_GHP_LEN = 4,   /* strlen("ghp_") */
    SECRET_GLPAT_LEN = 6, /* strlen("glpat-") */
    SECRET_XOX_LEN = 3,   /* strlen("xox"); the token-type letter follows */
    SECRET_XOX_DASH = 4,  /* offset of the '-' in "xoxb-" */
    SECRET_XOX_BODY = 5,  /* strlen("xoxb-") */
};

#include "pipeline/pipeline.h"
#include "pipeline/pipeline_internal.h"

#include <ctype.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

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

/* ── File identification ────────────────────────────────────────── */

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
    if (p && count_alnum(p + SECRET_AKIA_LEN) >= IS_TOKEN_MIN_1) {
        return true;
    }

    /* sk- + 20 alnum (API key) */
    p = ci_strstr(value, "sk-");
    if (p && count_alnum(p + SECRET_SK_LEN) >= IS_TOKEN_MIN_2) {
        return true;
    }

    /* ghp_ + 36 alnum (GitHub PAT) */
    p = ci_strstr(value, "ghp_");
    if (p && count_alnum(p + SECRET_GHP_LEN) >= GITHUB_PAT_MIN_ALNUM) {
        return true;
    }

    /* glpat- + 20 alnum/dash (GitLab PAT) */
    p = ci_strstr(value, "glpat-");
    if (p && count_alnum_dash(p + SECRET_GLPAT_LEN) >= IS_TOKEN_MIN_2) {
        return true;
    }

    /* xox[bps]- (Slack token) */
    p = ci_strstr(value, "xox");
    if (p && p[SECRET_XOX_LEN] != '\0' &&
        (tolower((unsigned char)p[SECRET_XOX_LEN]) == 'b' ||
         tolower((unsigned char)p[SECRET_XOX_LEN]) == 'p' ||
         tolower((unsigned char)p[SECRET_XOX_LEN]) == 's') &&
        p[SECRET_XOX_DASH] == '-' && count_alnum_dash(p + SECRET_XOX_BODY) >= SKIP_ONE) {
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

/* ── Helm Chart.yaml parser (#338) ───────────────────────────────── */

/* Copy the scalar value following a "key:" up to end-of-line into out,
 * trimming surrounding whitespace, optional quotes, and trailing comments. */
static void helm_copy_scalar(const char *s, const char *eol, char *out, size_t cap) {
    while (s < eol && (*s == ' ' || *s == '\t')) {
        s++;
    }
    char quote = 0;
    if (s < eol && (*s == '"' || *s == '\'')) {
        quote = *s;
        s++;
    }
    const char *e = s;
    while (e < eol) {
        if (quote) {
            if (*e == quote) {
                break;
            }
        } else if (*e == ' ' || *e == '\t' || *e == '#' || *e == '\r') {
            break;
        }
        e++;
    }
    size_t n = (size_t)(e - s);
    if (n >= cap) {
        n = cap - 1;
    }
    memcpy(out, s, n);
    out[n] = '\0';
}

/* Match "key:" at the start of a content pointer; return value start or NULL. */
static const char *helm_match_key(const char *content, const char *eol, const char *key) {
    size_t kl = strlen(key);
    if ((size_t)(eol - content) < kl + 1) {
        return NULL;
    }
    if (strncmp(content, key, kl) != 0 || content[kl] != ':') {
        return NULL;
    }
    return content + kl + 1;
}

/* Append an empty dependency entry; returns it, or NULL when out of memory. */
static cbm_helm_dep_t *helm_push_dep(cbm_helm_chart_t *out) {
    if (out->dep_count == out->dep_cap) {
        enum { HELM_DEPS_MIN = 8 };
        int cap = out->dep_cap ? out->dep_cap * 2 : HELM_DEPS_MIN;
        cbm_helm_dep_t *grown =
            cbm_realloc(CBM_MEM_CLASS_OTHER, out->deps, (size_t)cap * sizeof(*grown));
        if (!grown) {
            return NULL;
        }
        out->deps = grown;
        out->dep_cap = cap;
    }
    cbm_helm_dep_t *dep = &out->deps[out->dep_count++];
    dep->name[0] = '\0';
    dep->repository[0] = '\0';
    return dep;
}

void cbm_helm_chart_free(cbm_helm_chart_t *hc) {
    if (!hc) {
        return;
    }
    cbm_free(CBM_MEM_CLASS_OTHER, hc->deps);
    hc->deps = NULL;
    hc->dep_count = 0;
    hc->dep_cap = 0;
}

int cbm_parse_helm_chart(const char *source, cbm_helm_chart_t *out) {
    if (!out) {
        return -1;
    }
    out->chart_name[0] = '\0';
    out->deps = NULL;
    out->dep_count = 0;
    out->dep_cap = 0;
    if (!source) {
        return -1;
    }

    bool in_deps = false;
    cbm_helm_dep_t *cur = NULL; /* the dependency entry being read */
    int item_indent = -1;       /* indent of the entries' "- " */
    int key_col = -1;           /* column of an entry's keys */
    const char *p = source;
    while (*p) {
        const char *eol = strchr(p, '\n');
        if (!eol) {
            eol = p + strlen(p);
        }
        int indent = 0;
        const char *content = p;
        while (content < eol && *content == ' ') {
            content++;
            indent++;
        }
        if (content < eol && *content != '#') {
            if (indent == 0) {
                /* New top-level key ends any dependencies block. */
                in_deps = (helm_match_key(content, eol, "dependencies") != NULL);
                cur = NULL;
                item_indent = -1;
                key_col = -1;
                const char *nv = helm_match_key(content, eol, "name");
                if (!in_deps && nv && out->chart_name[0] == '\0') {
                    helm_copy_scalar(nv, eol, out->chart_name, CBM_HELM_NAME_MAX);
                }
            } else if (in_deps) {
                /* A list item ("- ") at the entries' indent opens an entry; its
                 * keys share the dash line or follow at the same key column.
                 * Deeper lines (import-values, tags) belong to the entry. */
                const char *q = content;
                bool opens = *q == '-' && (item_indent < 0 || indent == item_indent);
                if (opens) {
                    item_indent = indent;
                    q++;
                    while (q < eol && *q == ' ') {
                        q++;
                    }
                    key_col = indent + (int)(q - content);
                    cur = helm_push_dep(out);
                }
                if (cur && (opens || indent == key_col)) {
                    const char *nv = helm_match_key(q, eol, "name");
                    const char *rv = helm_match_key(q, eol, "repository");
                    if (nv) {
                        helm_copy_scalar(nv, eol, cur->name, CBM_HELM_NAME_MAX);
                    } else if (rv) {
                        helm_copy_scalar(rv, eol, cur->repository, CBM_HELM_REPO_MAX);
                    }
                }
            }
        }
        if (*eol == '\0') {
            break;
        }
        p = eol + 1;
    }
    /* Keep only named entries (a dependency without a name names nothing). */
    int kept = 0;
    for (int i = 0; i < out->dep_count; i++) {
        if (out->deps[i].name[0]) {
            out->deps[kept++] = out->deps[i];
        }
    }
    out->dep_count = kept;
    return (out->chart_name[0] || out->dep_count > 0) ? 0 : -1;
}
