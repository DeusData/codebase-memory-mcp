/*
 * lsp_env.h — shared environment flag parsing for light semantic passes.
 *
 * Debug/probe flags are process-wide and best-effort. Keep parsing consistent
 * across language resolvers and use the repository's safe getenv wrapper.
 */
#ifndef CBM_LSP_ENV_H
#define CBM_LSP_ENV_H

#include "foundation/constants.h"
#include "foundation/platform.h"

#include <stdbool.h>
#include <string.h>

static inline char cbm_lsp_ascii_lower(char c) {
    return (c >= 'A' && c <= 'Z') ? (char)(c + ('a' - 'A')) : c;
}

static inline bool cbm_lsp_ascii_eq_ignore_case(const char *a, const char *b) {
    if (!a || !b) {
        return false;
    }
    while (*a && *b) {
        if (cbm_lsp_ascii_lower(*a) != cbm_lsp_ascii_lower(*b)) {
            return false;
        }
        a++;
        b++;
    }
    return *a == '\0' && *b == '\0';
}

static inline bool cbm_lsp_env_flag_enabled(const char *name) {
    char buf[CBM_SZ_32];
    const char *value = cbm_safe_getenv(name, buf, sizeof(buf), NULL);
    if (!value || value[0] == '\0') {
        return false;
    }
    return strcmp(value, "0") != 0 && !cbm_lsp_ascii_eq_ignore_case(value, "false") &&
           !cbm_lsp_ascii_eq_ignore_case(value, "off") &&
           !cbm_lsp_ascii_eq_ignore_case(value, "no");
}

#endif /* CBM_LSP_ENV_H */
