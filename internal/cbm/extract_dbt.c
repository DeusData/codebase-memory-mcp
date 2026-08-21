// extract_dbt.c — dbt lineage extractor for Jinja-templated SQL models.
//
// A dbt model is an ordinary .sql file in the repository whose SELECT is
// templated with Jinja: dependencies are written `{{ ref('other_model') }}`
// (another model in the project) or `{{ source('group', 'table') }}` (a raw
// warehouse table), never as literal table names. The SQL grammar cannot read
// those — `FROM {{ ref('x') }}` is a parse error to it — so the dependency
// structure of an entire dbt project is invisible to the code graph without
// this pass.
//
// The vendored tree-sitter-jinja2 grammar models `{{ ... }}` expressions
// (jinja_expression / fn_call / lit_string) but has no node types at all for
// `{% ... %}` statements, so this extractor covers lineage only. `{% macro %}`
// definitions would need a hand-written scanner and are deliberately left out
// rather than recovered approximately.
//
// What it emits, per qualifying file:
//   - one Model definition, named by the file stem (dbt's own model identity)
//   - one usage per ref()/source() call, scoped to that Model
// pass_usages then resolves each usage against the shared definition registry
// and emits a `model -USAGE-> relation` lineage edge. Model is a relation label
// (cbm_label_is_relation), so lineage joins dbt models to Table/View nodes
// declared in plain DDL elsewhere in the same repository, and the registry's
// relation veto keeps these names out of every non-lineage consumer.
//
// Gate: the file must parse as SQL, contain a Jinja expression delimiter, and
// contain at least one real ref()/source() call. Generic templated SQL (an
// Airflow `{{ ds }}` parameter, say) therefore produces nothing at all — the
// dbt builtins are the evidence that this is a dbt model, so no dbt_project.yml
// lookup is needed and no non-dbt repository pays for a Model node it did not
// ask for.

#include "cbm.h"
#include "arena.h"
#include "helpers.h"
#include "lang_specs.h"
#include "tree_sitter/api.h"
#include "foundation/constants.h"
#include <stdbool.h>
#include <stdint.h>
#include <string.h>

/* Local constants. */
enum { DBT_FIRST_LINE = 1 };

/* Cheap pre-filter: does the source contain a Jinja expression opener? Files
 * without one cannot hold a ref()/source() call, and skipping them keeps the
 * second parse off every ordinary .sql file in the repository. */
static bool source_has_jinja_expr(const char *s, int len) {
    for (int i = 0; i + 1 < len; i++) {
        if (s[i] == '{' && s[i + 1] == '{') {
            return true;
        }
    }
    return false;
}

/* Strip one pair of surrounding quotes from a jinja lit_string. The grammar
 * hands back the token with its quotes attached. */
static char *dbt_unquote(CBMArena *a, char *s) {
    if (!s) {
        return NULL;
    }
    size_t n = strlen(s);
    if (n >= 2 && (s[0] == '\'' || s[0] == '"') && s[n - 1] == s[0]) {
        char *inner = cbm_arena_strdup(a, s + 1);
        if (!inner) {
            return s;
        }
        size_t m = strlen(inner);
        if (m > 0) {
            inner[m - 1] = '\0';
        }
        return inner;
    }
    return s;
}

/* Rightmost lit_string under `node` in DFS order. Both dbt builtins put the
 * referenced relation last: ref('model'), ref('package', 'model') and
 * source('group', 'table') all name the relation in their final string
 * argument. The callee itself is an identifier, never a lit_string, so it
 * cannot be mistaken for one. */
static TSNode dbt_last_lit_string(TSNode node, TSNode best, bool *found) {
    if (strcmp(ts_node_type(node), "lit_string") == 0) {
        best = node;
        *found = true;
    }
    uint32_t cc = ts_node_child_count(node);
    for (uint32_t i = 0; i < cc; i++) {
        best = dbt_last_lit_string(ts_node_child(node, i), best, found);
    }
    return best;
}

/* Collect ref()/source() targets from a jinja2 parse tree into `out`, which is
 * the file's usage array. Usages are scoped to enclosing_qn (the Model). */
static void collect_dbt_refs(CBMExtractCtx *ctx, TSNode node, const char *enclosing_qn,
                             CBMUsageArray *out) {
    if (strcmp(ts_node_type(node), "fn_call") == 0) {
        TSNode fn = ts_node_child_by_field_name(node, "fn_name", (uint32_t)strlen("fn_name"));
        if (ts_node_is_null(fn)) {
            fn = cbm_find_child_by_kind(node, "identifier");
        }
        if (!ts_node_is_null(fn)) {
            char *fname = cbm_node_text(ctx->arena, fn, ctx->source);
            if (fname && (strcmp(fname, "ref") == 0 || strcmp(fname, "source") == 0)) {
                bool found = false;
                TSNode empty = {0};
                TSNode strn = dbt_last_lit_string(node, empty, &found);
                if (found) {
                    char *name =
                        dbt_unquote(ctx->arena, cbm_node_text(ctx->arena, strn, ctx->source));
                    if (name && name[0]) {
                        CBMUsage usage = {0};
                        usage.ref_name = name;
                        usage.enclosing_func_qn = enclosing_qn;
                        usage.site_start_byte = ts_node_start_byte(strn);
                        usage.site_end_byte = ts_node_end_byte(strn);
                        cbm_usages_push(out, ctx->arena, usage);
                    }
                }
            }
        }
    }
    uint32_t cc = ts_node_child_count(node);
    for (uint32_t i = 0; i < cc; i++) {
        collect_dbt_refs(ctx, ts_node_child(node, i), enclosing_qn, out);
    }
}

/* dbt model identity is the file stem: models/staging/stg_users.sql is the
 * model `stg_users`, and `{{ ref('stg_users') }}` anywhere in the project
 * addresses it by that bare name (dbt requires model names to be unique across
 * a project, so the directory is deliberately not part of the identity). */
static char *dbt_name_from_path(CBMArena *a, const char *rel_path) {
    if (!rel_path) {
        return NULL;
    }
    const char *base = rel_path;
    for (const char *p = rel_path; *p; p++) {
        if (*p == '/' || *p == '\\') {
            base = p + 1;
        }
    }
    const char *dot = NULL;
    for (const char *p = base; *p; p++) {
        if (*p == '.') {
            dot = p;
        }
    }
    size_t n = dot ? (size_t)(dot - base) : strlen(base);
    if (n == 0) {
        return NULL;
    }
    char *out = cbm_arena_strdup(a, base);
    if (!out) {
        return NULL;
    }
    out[n] = '\0';
    return out;
}

void cbm_extract_dbt(CBMExtractCtx *ctx) {
    if (!ctx || ctx->language != CBM_LANG_SQL || !ctx->source || ctx->source_len <= 0) {
        return;
    }
    if (!source_has_jinja_expr(ctx->source, ctx->source_len)) {
        return;
    }
    const TSLanguage *jl = cbm_ts_language(CBM_LANG_JINJA2);
    if (!jl) {
        return;
    }
    char *model_name = dbt_name_from_path(ctx->arena, ctx->rel_path);
    if (!model_name || !model_name[0]) {
        return;
    }
    const char *model_qn = cbm_fqn_compute(ctx->arena, ctx->project, ctx->rel_path, model_name);
    if (!model_qn) {
        return;
    }

    /* A fresh parser: the primary SQL pass owns the thread-local one, and this
     * runs inside its walk. */
    TSParser *parser = ts_parser_new();
    if (!parser) {
        return;
    }
    /* Refs are staged locally so a file with Jinja but no dbt builtins commits
     * nothing at all — neither usages nor a Model node. */
    CBMUsageArray staged = {0};
    if (ts_parser_set_language(parser, jl)) {
        TSTree *tree = ts_parser_parse_string(parser, NULL, ctx->source, (uint32_t)ctx->source_len);
        if (tree) {
            collect_dbt_refs(ctx, ts_tree_root_node(tree), model_qn, &staged);
            ts_tree_delete(tree);
        }
    }
    ts_parser_delete(parser);
    if (staged.count == 0) {
        return; /* templated SQL, but not dbt — emit nothing */
    }

    CBMDefinition def;
    memset(&def, 0, sizeof(def));
    def.name = model_name;
    def.qualified_name = model_qn;
    def.label = "Model";
    def.file_path = ctx->rel_path;
    def.start_line = DBT_FIRST_LINE;
    def.end_line = ts_node_end_point(ctx->root).row + TS_LINE_OFFSET;
    def.is_exported = true;
    cbm_defs_push(&ctx->result->defs, ctx->arena, def);

    for (int i = 0; i < staged.count; i++) {
        cbm_usages_push(&ctx->result->usages, ctx->arena, staged.items[i]);
    }
}
