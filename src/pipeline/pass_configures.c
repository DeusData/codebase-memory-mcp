/*
 * pass_configures.c — Config key and env var helpers.
 *
 * Pure helper functions for detecting environment variable names,
 * normalizing config keys, and identifying config file extensions.
 */
#include "pipeline/pipeline.h"
#include "pipeline/pipeline_internal.h"

#include <ctype.h>
#include <string.h>

bool cbm_is_env_var_name(const char *s) {
    if (!s) {
        return false;
    }
    size_t len = strlen(s);
    if (len < PAIR_LEN) {
        return false;
    }

    bool has_upper = false;
    for (size_t i = 0; i < len; i++) {
        char c = s[i];
        if (c >= 'A' && c <= 'Z') {
            has_upper = true;
        } else if (c == '_' || (c >= '0' && c <= '9')) {
            /* ok */
        } else {
            return false;
        }
    }
    return has_upper;
}

/* Emit a camelCase-split word (lowercased) into the output buffer.
 * Prepends '_' if out_pos > 0. Returns updated out_pos and token_count. */
static void emit_camel_words(const char *part, size_t plen, char *norm_out, size_t norm_sz,
                             size_t *out_pos, int *token_count) {
    size_t start = 0;
    for (size_t i = SKIP_ONE; i < plen; i++) {
        if (part[i] >= 'A' && part[i] <= 'Z' && part[i - SKIP_ONE] >= 'a' &&
            part[i - SKIP_ONE] <= 'z') {
            if (*out_pos > 0 && *out_pos < norm_sz - SKIP_ONE) {
                norm_out[(*out_pos)++] = '_';
            }
            for (size_t j = start; j < i && *out_pos < norm_sz - SKIP_ONE; j++) {
                norm_out[(*out_pos)++] = (char)tolower((unsigned char)part[j]);
            }
            (*token_count)++;
            start = i;
        }
    }
    /* Emit remaining */
    if (*out_pos > 0 && *out_pos < norm_sz - SKIP_ONE) {
        norm_out[(*out_pos)++] = '_';
    }
    for (size_t j = start; j < plen && *out_pos < norm_sz - SKIP_ONE; j++) {
        norm_out[(*out_pos)++] = (char)tolower((unsigned char)part[j]);
    }
    (*token_count)++;
}

int cbm_normalize_config_key(const char *key, char *norm_out, size_t norm_sz) {
    if (!key || !norm_out || norm_sz == 0) {
        return 0;
    }
    norm_out[0] = '\0';

    char buf[CBM_SZ_512];
    size_t klen = strlen(key);
    if (klen >= sizeof(buf)) {
        klen = sizeof(buf) - SKIP_ONE;
    }
    memcpy(buf, key, klen);
    buf[klen] = '\0';

    /* Replace delimiters with spaces */
    for (size_t i = 0; i < klen; i++) {
        if (buf[i] == '_' || buf[i] == '-' || buf[i] == '.') {
            buf[i] = ' ';
        }
    }

    int token_count = 0;
    size_t out_pos = 0;

    char *saveptr = NULL;
    char *part = strtok_r(buf, " ", &saveptr);
    while (part) {
        emit_camel_words(part, strlen(part), norm_out, norm_sz, &out_pos, &token_count);
        part = strtok_r(NULL, " ", &saveptr);
    }

    norm_out[out_pos] = '\0';
    return token_count;
}

bool cbm_has_config_extension(const char *path) {
    if (!path) {
        return false;
    }

    /* Find last dot */
    const char *dot = strrchr(path, '.');
    const char *basename = strrchr(path, '/');
    if (!basename) {
        basename = path;
    } else {
        basename++;
    }

    /* Special case: .env files */
    if (strcmp(basename, ".env") == 0) {
        return true;
    }

    if (!dot) {
        return false;
    }

    static const char *exts[] = {".toml", ".ini", ".yaml", ".yml", ".cfg", ".properties",
                                 ".json", ".xml", ".conf", ".env", NULL};

    for (int i = 0; exts[i]; i++) {
        if (strcmp(dot, exts[i]) == 0) {
            return true;
        }
    }
    return false;
}
