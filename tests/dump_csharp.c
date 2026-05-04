/* dump_csharp.c — Standalone inspector for C# extraction.
 * Reads a file path, runs cbm_extract_file, prints calls/type_assigns/defs.
 */
#include "cbm.h"
#include "tree_sitter/api.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

extern const TSLanguage *tree_sitter_c_sharp(void);

static void walk(TSNode n, int depth, const char *src, int max_depth) {
    if (depth > max_depth) return;
    const char *kind = ts_node_type(n);
    uint32_t sb = ts_node_start_byte(n);
    uint32_t eb = ts_node_end_byte(n);
    char preview[64];
    int plen = (int)(eb - sb);
    if (plen > 60) plen = 60;
    memcpy(preview, src + sb, plen);
    preview[plen] = 0;
    for (int i = 0; i < plen; i++) if (preview[i] == '\n') preview[i] = ' ';
    printf("%*s%s [%.*s]\n", depth * 2, "", kind, plen, preview);
    uint32_t nc = ts_node_child_count(n);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode c = ts_node_child(n, i);
        const char *fld = ts_node_field_name_for_child(n, i);
        if (fld) printf("%*s.%s:\n", (depth + 1) * 2, "", fld);
        walk(c, depth + 1, src, max_depth);
    }
}

static char *slurp(const char *path, int *len_out) {
    FILE *f = fopen(path, "rb");
    if (!f) return NULL;
    fseek(f, 0, SEEK_END);
    long n = ftell(f);
    fseek(f, 0, SEEK_SET);
    char *buf = malloc((size_t)n + 1);
    fread(buf, 1, (size_t)n, f);
    buf[n] = 0;
    *len_out = (int)n;
    fclose(f);
    return buf;
}

int main(int argc, char **argv) {
    if (argc < 2) {
        fprintf(stderr, "usage: %s <file.cs>\n", argv[0]);
        return 1;
    }
    if (cbm_init() != 0) {
        fprintf(stderr, "cbm_init failed\n");
        return 1;
    }
    int n = 0;
    char *src = slurp(argv[1], &n);
    if (!src) {
        fprintf(stderr, "cannot read %s\n", argv[1]);
        return 1;
    }
    CBMFileResult *r = cbm_extract_file(src, n, CBM_LANG_CSHARP, "test", argv[1], 0, NULL, NULL);
    if (!r) { fprintf(stderr, "extract returned NULL\n"); return 1; }
    if (r->has_error) {
        fprintf(stderr, "parse error: %s\n", r->error_msg ? r->error_msg : "?");
    }

    printf("=== DEFS (%d) ===\n", r->defs.count);
    for (int i = 0; i < r->defs.count; i++) {
        const CBMDefinition *d = &r->defs.items[i];
        printf("  [%s] %s qn=%s parent=%s rt=%s",
               d->label ? d->label : "?", d->name ? d->name : "?",
               d->qualified_name ? d->qualified_name : "",
               d->parent_class ? d->parent_class : "",
               d->return_type ? d->return_type : "");
        printf(" params=");
        if (d->param_names) {
            printf("[");
            for (int j = 0; d->param_names[j]; j++) {
                printf("%s%s:%s", j ? "," : "", d->param_names[j],
                       d->param_types && d->param_types[j] ? d->param_types[j] : "?");
            }
            printf("]");
        } else {
            printf("NULL");
        }
        printf(" sig=%s\n", d->signature ? d->signature : "NULL");
    }

    printf("=== CALLS (%d) ===\n", r->calls.count);
    for (int i = 0; i < r->calls.count; i++) {
        const CBMCall *c = &r->calls.items[i];
        printf("  callee=%s  enc=%s\n",
               c->callee_name ? c->callee_name : "?",
               c->enclosing_func_qn ? c->enclosing_func_qn : "");
    }

    printf("=== TYPE_ASSIGNS (%d) ===\n", r->type_assigns.count);
    for (int i = 0; i < r->type_assigns.count; i++) {
        const CBMTypeAssign *t = &r->type_assigns.items[i];
        printf("  var=%s type=%s enc=%s\n",
               t->var_name ? t->var_name : "?",
               t->type_name ? t->type_name : "?",
               t->enclosing_func_qn ? t->enclosing_func_qn : "");
    }

    if (argc >= 3 && strcmp(argv[2], "--ast") == 0) {
        printf("\n=== AST ===\n");
        TSParser *p = ts_parser_new();
        ts_parser_set_language(p, tree_sitter_c_sharp());
        TSTree *tree = ts_parser_parse_string(p, NULL, src, n);
        walk(ts_tree_root_node(tree), 0, src, 8);
        ts_tree_delete(tree);
        ts_parser_delete(p);
    }

    cbm_free_result(r);
    free(src);
    return 0;
}
