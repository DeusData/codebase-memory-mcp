#include "macro_table.h"
#include "arena.h"
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <stdbool.h>

static const struct {
    const char *name;
    int param_count;
    const char *callee;
} SYSTEM_MACROS[] = {{"OK", 0, NULL},
                     {"ISERR", 1, "%SYSTEM.Status.IsError"},
                     {"ISOK", 1, "%SYSTEM.Status.IsOK"},
                     {"GETERRORTEXT", 1, "%SYSTEM.Status.GetErrorText"},
                     {"ADDSC", 2, "%SYSTEM.Status.AppendStatus"},
                     {"ThrowStatus", 1, "%SYSTEM.Status.ThrowStatus"},
                     {"ThrowOnError", 1, "%SYSTEM.Status.ThrowStatus"},
                     {"ERROR", 2, "%SYSTEM.Status.Error"},
                     {"NULLOREF", 0, NULL},
                     {"LISTBUILD", -1, NULL},
                     {"LISTGET", 2, NULL},
                     {"LISTNEXT", 3, NULL},
                     {"LISTLENGTH", 1, NULL},
                     {"SORTBEGIN", 1, NULL},
                     {"SORTEND", 0, NULL},
                     {"AUDITSTART", 3, "%SYSTEM.Audit.Event"},
                     {"logoutput", 1, NULL},
                     {"objExists", 1, NULL},
                     {"traceStatus", 1, NULL},
                     {NULL, 0, NULL}};

void cbm_macro_table_free(CBMMacroTable *t) {
    if (!t)
        return;
    cbm_arena_destroy(&t->arena);
    free(t);
}

void cbm_macro_table_init_system(CBMMacroTable *t) {
    t->count = 0;
    for (int i = 0; SYSTEM_MACROS[i].name; i++) {
        if (t->count >= CBM_MACRO_TABLE_CAP)
            break;
        CBMMacroEntry *e = &t->entries[t->count++];
        e->name = SYSTEM_MACROS[i].name;
        e->param_count = SYSTEM_MACROS[i].param_count;
        e->expansion = NULL;
        e->resolved_callee = SYSTEM_MACROS[i].callee;
        for (int p = 0; p < CBM_MACRO_MAX_PARAMS; p++)
            e->param_names[p] = NULL;
    }
}

void cbm_macro_table_add(CBMMacroTable *t, CBMArena *arena, const char *name, int param_count,
                         const char **param_names, const char *expansion,
                         const char *resolved_callee) {
    if (t->count >= CBM_MACRO_TABLE_CAP || !name)
        return;
    for (int i = 0; i < t->count; i++) {
        if (strcasecmp(t->entries[i].name, name) == 0)
            return;
    }
    CBMMacroEntry *e = &t->entries[t->count++];
    e->name = cbm_arena_strdup(arena, name);
    e->param_count = param_count;
    e->expansion = expansion ? cbm_arena_strdup(arena, expansion) : NULL;
    e->resolved_callee = resolved_callee ? cbm_arena_strdup(arena, resolved_callee) : NULL;
    for (int p = 0; p < CBM_MACRO_MAX_PARAMS; p++) {
        e->param_names[p] =
            (param_names && p < param_count) ? cbm_arena_strdup(arena, param_names[p]) : NULL;
    }
}

const CBMMacroEntry *cbm_macro_table_find(const CBMMacroTable *t, const char *name) {
    if (!t || !name)
        return NULL;
    for (int i = 0; i < t->count; i++) {
        if (strcasecmp(t->entries[i].name, name) == 0)
            return &t->entries[i];
    }
    return NULL;
}

void cbm_parse_inc_file(CBMMacroTable *t, CBMArena *arena, const char *content) {
    if (!content)
        return;
    const char *line = content;
    while (*line) {
        const char *end = strchr(line, '\n');
        if (!end)
            end = line + strlen(line);

        const char *p = line;
        while (*p == ' ' || *p == '\t')
            p++;

        if (strncmp(p, "#define", 7) == 0 && (p[7] == ' ' || p[7] == '\t')) {
            p += 8;
            while (*p == ' ' || *p == '\t')
                p++;

            const char *name_start = p;
            while (*p && *p != '(' && *p != ' ' && *p != '\t' && p < end)
                p++;
            if (p == name_start)
                goto next_line;

            char name[256];
            int nlen = (int)(p - name_start);
            if (nlen >= (int)sizeof(name))
                goto next_line;
            memcpy(name, name_start, nlen);
            name[nlen] = '\0';

            int param_count = -1;
            char param_names_buf[CBM_MACRO_MAX_PARAMS][64];
            const char *param_name_ptrs[CBM_MACRO_MAX_PARAMS] = {NULL};

            if (*p == '(') {
                param_count = 0;
                p++;
                while (*p && *p != ')' && p < end) {
                    while (*p == ' ' || *p == '\t')
                        p++;
                    if (*p == ')')
                        break;
                    const char *pn_start = p;
                    while (*p && *p != ',' && *p != ')' && p < end)
                        p++;
                    int plen = (int)(p - pn_start);
                    while (plen > 0 && (pn_start[plen - 1] == ' ' || pn_start[plen - 1] == '\t'))
                        plen--;
                    if (plen > 0 && param_count < CBM_MACRO_MAX_PARAMS) {
                        memcpy(param_names_buf[param_count], pn_start, plen < 63 ? plen : 63);
                        param_names_buf[param_count][plen < 63 ? plen : 63] = '\0';
                        param_name_ptrs[param_count] = param_names_buf[param_count];
                        param_count++;
                    }
                    if (*p == ',')
                        p++;
                }
                if (*p == ')')
                    p++;
            }

            while (*p == ' ' || *p == '\t')
                p++;
            int explen = (int)(end - p);
            while (explen > 0 &&
                   (p[explen - 1] == '\r' || p[explen - 1] == ' ' || p[explen - 1] == '\t'))
                explen--;
            char *expansion = NULL;
            if (explen > 0) {
                expansion = cbm_arena_strndup(arena, p, explen);
            }

            cbm_macro_table_add(t, arena, name, param_count,
                                param_count > 0 ? param_name_ptrs : NULL, expansion, NULL);
        }

    next_line:
        if (!*end)
            break;
        line = end + 1;
    }
}

char *cbm_macro_expand(CBMArena *arena, const CBMMacroEntry *entry, const char **args,
                       int arg_count) {
    if (!entry || !entry->expansion)
        return NULL;
    const char *tmpl = entry->expansion;
    char buf[1024];
    int out = 0;
    const char *p = tmpl;
    while (*p && out < (int)sizeof(buf) - 1) {
        if (*p == '%') {
            bool matched = false;
            for (int i = 0; i < entry->param_count && i < CBM_MACRO_MAX_PARAMS; i++) {
                if (!entry->param_names[i])
                    continue;
                int pnlen = (int)strlen(entry->param_names[i]);
                if (strncasecmp(p, entry->param_names[i], pnlen) == 0) {
                    const char *arg = (args && i < arg_count) ? args[i] : "";
                    int alen = (int)strlen(arg);
                    if (out + alen < (int)sizeof(buf) - 1) {
                        memcpy(buf + out, arg, alen);
                        out += alen;
                    }
                    p += pnlen;
                    matched = true;
                    break;
                }
            }
            if (!matched)
                buf[out++] = *p++;
        } else {
            buf[out++] = *p++;
        }
    }
    buf[out] = '\0';
    return cbm_arena_strdup(arena, buf);
}

char *cbm_macro_extract_callee(CBMArena *arena, const char *expansion) {
    if (!expansion)
        return NULL;

    const char *p = strstr(expansion, "##class(");
    if (p) {
        p += 8;
        const char *cls_end = strchr(p, ')');
        if (!cls_end)
            return NULL;
        int clen = (int)(cls_end - p);
        const char *dot = cls_end + 1;
        if (*dot != '.')
            return NULL;
        dot++;
        const char *method_start = dot;
        const char *method_end = method_start;
        while (*method_end && *method_end != '(' && *method_end != ' ')
            method_end++;
        int mlen = (int)(method_end - method_start);
        if (clen <= 0 || mlen <= 0)
            return NULL;
        return cbm_arena_sprintf(arena, "%.*s.%.*s", clen, p, mlen, method_start);
    }

    p = strstr(expansion, "$$");
    if (p && p[2] != '$') {
        p += 2;
        const char *tag_end = p;
        while (*tag_end && *tag_end != '^' && *tag_end != '(' && *tag_end != ' ')
            tag_end++;
        if (*tag_end == '^') {
            const char *rtn = tag_end + 1;
            const char *rtn_end = rtn;
            while (*rtn_end && *rtn_end != '(' && *rtn_end != ' ')
                rtn_end++;
            int tlen = (int)(tag_end - p);
            int rlen = (int)(rtn_end - rtn);
            if (tlen > 0 && rlen > 0) {
                return cbm_arena_sprintf(arena, "%.*s^%.*s", tlen, p, rlen, rtn);
            }
        }
    }

    return NULL;
}
