/*
 * helm_template.h -- Helm values and the rendering of a chart template's
 * image references (pass_k8s.c). Internal to the pipeline.
 *
 * Values are JSON (yyjson mutable values), as Helm holds them after its
 * YAML -> JSON step: go-yaml v2 scalars (YAML 1.1 booleans), every number a
 * float64. A template renders with Go text/template semantics and the part
 * of Sprig image expressions use; anything else -- a function not modelled,
 * .Release / .Capabilities / lookup, a missing helper, a parse error on the
 * way -- is UNKNOWN, and an image that reads an UNKNOWN is no image. Nothing
 * is guessed: a value is what Helm would render, or none.
 */
#ifndef CBM_HELM_TEMPLATE_H
#define CBM_HELM_TEMPLATE_H

#include "foundation/arena.h"

#include "tree_sitter/api.h"
#include <yyjson/yyjson.h>

#include <stdbool.h>
#include <stddef.h>

/* The first document of YAML text src as a value of doc (Helm's scalar
 * types); NULL when it does not parse. An empty document is an empty map. */
yyjson_mut_val *cbm_helm_values_parse(yyjson_mut_doc *doc, const char *src, size_t len);

/* The YAML node `node` of text `src` (already parsed, e.g. a HelmRelease's
 * spec.values) as a value of doc; NULL when it holds an unknown part. */
yyjson_mut_val *cbm_helm_values_node(yyjson_mut_doc *doc, const char *src, TSNode node);

/* Helm's coalesce: a deep copy of `base` with `over` merged in -- maps merge
 * key by key, any other value replaces, a null deletes. Either may be NULL. */
yyjson_mut_val *cbm_helm_values_coalesce(yyjson_mut_doc *doc, yyjson_mut_val *base,
                                         yyjson_mut_val *over);

/* Helm --set NAME=VALUE into map `root`: NAME's `.` separates keys (`\.`
 * escapes one), `[n]` indexes a list; VALUE is typed like --set (int, bool,
 * null, else string) unless `as_string`. False when NAME is malformed. */
bool cbm_helm_values_set(yyjson_mut_doc *doc, yyjson_mut_val *root, const char *name,
                         const char *value, bool as_string);

/* A values scalar as a template prints it (a number like Go's %v: 1.10 ->
 * 1.1) into out; false for a map, a list or null. */
bool cbm_helm_scalar_text(yyjson_mut_val *v, char *out, size_t sz);

/* A chart's metadata, as its templates see it under .Chart -- and where its
 * templates are: .Template.BasePath as Helm sets it (`<chart path>/templates`,
 * the chart path its root's name and `/charts/<name or alias>` per subchart
 * below it; NULL: not known) and the chart's directory as its template files
 * are added to the defines ("" at the repository root). */
typedef struct {
    const char *name;
    const char *version;
    const char *app_version;
    const char *base_path;
    const char *dir;
} cbm_helm_chart_meta_t;

/* The named templates (`define` / `block`) of one chart family -- a chart and
 * the charts below it share one namespace; for one name the define of the
 * file nearest the family root wins (Helm loads deeper files first), then
 * the alphabetically first path. */
typedef struct cbm_helm_defines cbm_helm_defines_t;

cbm_helm_defines_t *cbm_helm_defines_new(void);
/* Register the defines of template file `rel` (text src, len). The text is
 * copied; its parse lives as long as the set. */
void cbm_helm_defines_add(cbm_helm_defines_t *d, const char *rel, const char *src, size_t len);
void cbm_helm_defines_free(cbm_helm_defines_t *d);

/* A probe: the `image:` key on a template line. Each time rendering passes
 * that line it captures the scalar written after the key. */
typedef struct {
    int line;           /* 1-based line of the key in the template */
    int n;              /* captures */
    int cap;            /* room in image / keys (the render's own) */
    const char **image; /* the rendered scalar, YAML quoting removed; NULL: UNKNOWN */
    const char **keys;  /* JSON array of the values paths it read; NULL: none */
} cbm_helm_probe_t;

/* Render template `rel` (text src, len) with `values` (its .Values; NULL:
 * an empty map), `chart` and the family's defines, filling `probes` (strings
 * in arena a, which gets only these: the render works in an arena of its own).
 * Values paths are dot-joined keys, a key's own `.` escaped (`\.`), as --set
 * writes them. False when the render ran past its step, output or memory
 * budget -- a template Helm would not finish either (logged): then every
 * capture is UNKNOWN. */
bool cbm_helm_render(CBMArena *a, const char *rel, const char *src, size_t len,
                     yyjson_mut_val *values, const cbm_helm_chart_meta_t *chart,
                     const cbm_helm_defines_t *defs, cbm_helm_probe_t *probes, int nprobes);

#endif /* CBM_HELM_TEMPLATE_H */
