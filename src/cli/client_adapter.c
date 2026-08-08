/*
 * client_adapter.c — Generate client extension modules from the MCP tool registry.
 *
 * See client_adapter.h for how generation is split between the verified
 * template (module scaffolding) and the live registry (tool list).
 */
#include "cli/client_adapter.h"

#include "cli/integration_assets.h"
#include "foundation/constants.h"
#include "mcp/mcp.h"

#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* A generated module is small and bounded by the registry size; grow a plain
 * buffer rather than pulling in a builder for two call sites. */
typedef struct {
    char *buf;
    size_t len;
    size_t cap;
    bool failed;
} adapter_sb_t;

static void sb_append_len(adapter_sb_t *sb, const char *s, size_t add) {
    if (sb->failed || !s) {
        return;
    }
    if (add == 0U) {
        return;
    }
    if (sb->len == SIZE_MAX || add > SIZE_MAX - sb->len - 1U) {
        sb->failed = true;
        return;
    }
    size_t required = sb->len + add + 1U;
    if (!sb->buf || required > sb->cap) {
        size_t want = sb->cap ? sb->cap : CBM_SZ_2K;
        while (want < required) {
            if (want > SIZE_MAX / PAIR_LEN) {
                sb->failed = true;
                return;
            }
            want *= PAIR_LEN;
        }
        char *grown = realloc(sb->buf, want);
        if (!grown) {
            sb->failed = true;
            return;
        }
        sb->buf = grown;
        sb->cap = want;
    }
    memcpy(sb->buf + sb->len, s, add);
    sb->len += add;
    sb->buf[sb->len] = '\0';
}

static void sb_append(adapter_sb_t *sb, const char *s) {
    if (!sb->failed && s) {
        sb_append_len(sb, s, strlen(s));
    }
}

/* Append template text, substituting each named placeholder with its value.
 * An unresolved "{{" fails the build: a placeholder must never survive into
 * a module a client will execute. */
static void sb_append_template(adapter_sb_t *sb, const char *text, const char *const names[],
                               const char *const values[], size_t pair_count) {
    if (sb->failed || !text) {
        sb->failed = true;
        return;
    }
    const char *cursor = text;
    while (!sb->failed && *cursor) {
        const char *brace = strstr(cursor, "{{");
        if (!brace) {
            sb_append(sb, cursor);
            return;
        }
        sb_append_len(sb, cursor, (size_t)(brace - cursor));
        size_t matched = 0U;
        for (size_t i = 0U; i < pair_count; i++) {
            size_t name_length = strlen(names[i]);
            if (strncmp(brace + PAIR_LEN, names[i], name_length) == 0 &&
                strncmp(brace + PAIR_LEN + name_length, "}}", PAIR_LEN) == 0) {
                sb_append(sb, values[i]);
                matched = PAIR_LEN + name_length + PAIR_LEN;
                break;
            }
        }
        if (matched == 0U) {
            sb->failed = true;
            return;
        }
        cursor = brace + matched;
    }
}

bool cbm_client_adapter_escape_js(const char *in, char *out, size_t out_sz) {
    if (!in || !out || out_sz == 0) {
        return false;
    }
    size_t o = 0;
    for (const char *p = in; *p; p++) {
        /* Only these four can terminate or corrupt a single-quoted literal.
         * Escaping more would be harmless but makes the emitted path harder to
         * read when a user opens the generated file. */
        const char *esc = NULL;
        switch (*p) {
        case '\\':
            esc = "\\\\";
            break;
        case '\'':
            esc = "\\'";
            break;
        case '\n':
            esc = "\\n";
            break;
        case '\r':
            esc = "\\r";
            break;
        default:
            break;
        }
        size_t need = esc ? strlen(esc) : 1U;
        if (o + need + 1 > out_sz) {
            out[0] = '\0';
            return false; /* never emit a truncated literal */
        }
        if (esc) {
            memcpy(out + o, esc, need);
        } else {
            out[o] = *p;
        }
        o += need;
    }
    out[o] = '\0';
    return true;
}

/* The module scaffolding (spawn bridge, header note, plugin surface) comes
 * from the hash-verified cbm-integrations.json — those child-process-spawning
 * bytes must not live inside the binary (AV surface) — while the tool list is
 * still generated from the live registry, so the adapter cannot drift from
 * the tools the server actually serves. No markers in the template: the
 * managed-block writer adds them and REJECTS content that already carries
 * them, so the body must stay marker-free. */
char *cbm_client_adapter_pi(const char *binary_path) {
    if (!binary_path || !binary_path[0]) {
        return NULL;
    }
    char bin[CBM_SZ_1K];
    if (!cbm_client_adapter_escape_js(binary_path, bin, sizeof(bin))) {
        return NULL;
    }
    int count = cbm_mcp_tool_count();
    if (count <= 0) {
        return NULL;
    }
    const cbm_integration_template_t *tpl = cbm_integration_template("adapter_pi");
    if (!tpl || !tpl->tool_line || !tpl->current.bin_mode ||
        strcmp(tpl->current.bin_mode, "js") != 0) {
        return NULL;
    }

    /* Register every tool the registry advertises — never a hand-picked
     * subset, which is the drift this generator exists to prevent. */
    adapter_sb_t tools = {0};
    for (int i = 0; i < count; i++) {
        const char *name = cbm_mcp_tool_name(i);
        if (!name || !name[0]) {
            continue;
        }
        const char *const tool_names[] = {"TOOL"};
        const char *const tool_values[] = {name};
        sb_append_template(&tools, tpl->tool_line, tool_names, tool_values, 1U);
    }
    if (tools.failed) {
        free(tools.buf);
        return NULL;
    }

    adapter_sb_t sb = {0};
    const char *const names[] = {"BIN", "TOOLS"};
    const char *const values[] = {bin, tools.buf ? tools.buf : ""};
    sb_append_template(&sb, tpl->current.text, names, values, PAIR_LEN);
    free(tools.buf);
    if (sb.failed) {
        free(sb.buf);
        return NULL;
    }
    return sb.buf;
}

char *cbm_client_adapter_opencode(const char *binary_path) {
    if (!binary_path || !binary_path[0]) {
        return NULL;
    }
    char bin[CBM_SZ_1K];
    if (!cbm_client_adapter_escape_js(binary_path, bin, sizeof(bin))) {
        return NULL;
    }
    const cbm_integration_template_t *tpl = cbm_integration_template("adapter_opencode");
    if (!tpl || !tpl->current.bin_mode || strcmp(tpl->current.bin_mode, "js") != 0) {
        return NULL;
    }
    adapter_sb_t sb = {0};
    const char *const names[] = {"BIN"};
    const char *const values[] = {bin};
    sb_append_template(&sb, tpl->current.text, names, values, 1U);
    if (sb.failed) {
        free(sb.buf);
        return NULL;
    }
    return sb.buf;
}
