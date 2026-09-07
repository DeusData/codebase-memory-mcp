/* atlas_handout.c — the codebase handout: a self-contained HTML document
 * that explains a project to a newcomer, generated purely from the graph.
 *
 * Every fact is deterministic (regions, spanners, flows, churn×complexity);
 * no model-written prose. Sections that would only say "none" for this
 * project are omitted, caps are declared inline, and no host paths leak
 * into the artifact (it is meant to be shared). */

#include "foundation/constants.h"
#include "ui/atlas.h"
#include "ui/layout3d.h"

#include <yyjson/yyjson.h>

#include <stdbool.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

/* ── Growable output buffer ─────────────────────────────────────── */

typedef struct {
    char *data;
    size_t len;
    size_t cap;
    bool oom;
} hd_buf_t;

static void hd_put(hd_buf_t *b, const char *s) {
    if (b->oom || !s)
        return;
    size_t n = strlen(s);
    if (b->len + n + 1 > b->cap) {
        size_t nc = b->cap ? b->cap * 2 : 16384;
        while (nc < b->len + n + 1)
            nc *= 2;
        char *grown = realloc(b->data, nc);
        if (!grown) {
            b->oom = true;
            return;
        }
        b->data = grown;
        b->cap = nc;
    }
    memcpy(b->data + b->len, s, n);
    b->len += n;
    b->data[b->len] = '\0';
}

static void hd_putf(hd_buf_t *b, const char *fmt, ...) {
    char tmp[CBM_SZ_2K];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(tmp, sizeof(tmp), fmt, ap);
    va_end(ap);
    hd_put(b, tmp);
}

/* HTML-escape into the buffer — every graph-derived string passes here. */
static void hd_esc(hd_buf_t *b, const char *s) {
    if (!s)
        return;
    for (const char *c = s; *c; c++) {
        switch (*c) {
        case '&':
            hd_put(b, "&amp;");
            break;
        case '<':
            hd_put(b, "&lt;");
            break;
        case '>':
            hd_put(b, "&gt;");
            break;
        case '"':
            hd_put(b, "&quot;");
            break;
        default: {
            char one[2] = {*c, 0};
            hd_put(b, one);
        }
        }
    }
}

static const char *hd_cohesion_word(double cohesion) {
    if (cohesion >= 0.7)
        return "tightly connected";
    if (cohesion >= 0.4)
        return "moderately connected";
    return "loosely connected";
}

/* ── The document ───────────────────────────────────────────────── */

char *cbm_atlas_handout_html(cbm_store_t *store, const char *project) {
    if (!store || !project)
        return NULL;

    char *regions_json = cbm_layout_regions_json(store, project);
    char *metrics_json = cbm_atlas_metrics_json(store, project);
    char *flows_json = cbm_atlas_flows_json(store, project);
    char *bridges_json = cbm_atlas_bridges_json(store, project);

    yyjson_doc *regions_doc =
        regions_json ? yyjson_read(regions_json, strlen(regions_json), 0) : NULL;
    yyjson_doc *metrics_doc =
        metrics_json ? yyjson_read(metrics_json, strlen(metrics_json), 0) : NULL;
    yyjson_doc *flows_doc = flows_json ? yyjson_read(flows_json, strlen(flows_json), 0) : NULL;
    yyjson_doc *bridges_doc =
        bridges_json ? yyjson_read(bridges_json, strlen(bridges_json), 0) : NULL;

    hd_buf_t b = {0};
    hd_put(&b, "<!doctype html>\n<html lang=\"en\">\n<head>\n<meta charset=\"utf-8\">\n"
               "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\n");
    hd_put(&b, "<title>");
    hd_esc(&b, project);
    hd_put(&b, " — codebase handout</title>\n<style>\n"
               "body{margin:0;background:#FBFCFD;color:#1A2128;font:15px/1.55 -apple-system,"
               "'Segoe UI',Roboto,Helvetica,Arial,sans-serif;}\n"
               "main{max-width:860px;margin:0 auto;padding:40px 24px 80px;}\n"
               "h1{font-size:26px;line-height:1.15;margin:0 0 6px;}\n"
               "h2{font-size:18px;margin:36px 0 10px;border-bottom:1px solid #D7DEE4;"
               "padding-bottom:5px;}\n"
               "p{margin:0 0 12px;}\n"
               ".muted{color:#5C6870;font-size:13px;}\n"
               ".accent{color:#1E7FB8;}\n"
               "table{border-collapse:collapse;width:100%;font-size:13.5px;margin:10px 0 4px;}\n"
               "th{font-size:11.5px;text-transform:uppercase;letter-spacing:.06em;"
               "color:#5C6870;text-align:left;padding:6px 10px;border-bottom:1px solid #C9D1D8;}\n"
               "td{padding:5px 10px;border-bottom:1px solid #E4E9EE;vertical-align:top;}\n"
               "td.num{text-align:right;font-variant-numeric:tabular-nums;"
               "font-family:ui-monospace,Menlo,Consolas,monospace;font-size:12.5px;}\n"
               "code{font-family:ui-monospace,Menlo,Consolas,monospace;font-size:.9em;"
               "background:#EEF2F5;padding:1px 4px;border-radius:3px;}\n"
               ".why{color:#5C6870;font-size:13px;margin-top:2px;}\n"
               "</style>\n</head>\n<body>\n<main>\n");

    /* Header */
    hd_put(&b, "<h1>");
    hd_esc(&b, project);
    hd_put(&b, "</h1>\n<p class=\"muted\">A handout for newcomers, generated from the code "
               "graph. Every number below is computed from indexed structure — no summaries "
               "were written by a model. Verify anything with the "
               "<code>codebase-memory-mcp</code> tools.</p>\n");

    long long total_nodes = 0, unmapped = 0;
    yyjson_val *regions_arr = NULL;
    if (regions_doc) {
        yyjson_val *root = yyjson_doc_get_root(regions_doc);
        total_nodes = yyjson_get_int(yyjson_obj_get(root, "total_nodes"));
        unmapped = yyjson_get_int(yyjson_obj_get(root, "unmapped_nodes"));
        regions_arr = yyjson_obj_get(root, "regions");
    }
    if (regions_arr) {
        hd_putf(&b, "<p class=\"muted\">%lld symbols in %zu regions", total_nodes,
                yyjson_arr_size(regions_arr));
        if (unmapped > 0)
            hd_putf(&b, " (%lld outside any region)", unmapped);
        hd_put(&b, ".</p>\n");
    }

    /* The map */
    if (regions_arr && yyjson_arr_size(regions_arr) > 0) {
        hd_put(&b, "<h2>The map — regions of related code</h2>\n"
                   "<p class=\"why\">Regions are call communities: code that talks to itself "
                   "far more than to the rest. Names come from the dominant folder; the hub "
                   "is the most-connected symbol inside.</p>\n<table>\n<tr><th>Region</th>"
                   "<th>Hub</th><th>Symbols</th><th>Files</th><th>Cohesion</th></tr>\n");
        size_t shown = 0;
        for (size_t i = 0; i < yyjson_arr_size(regions_arr) && shown < 20; i++, shown++) {
            yyjson_val *rv = yyjson_arr_get(regions_arr, i);
            hd_put(&b, "<tr><td>");
            hd_esc(&b, yyjson_get_str(yyjson_obj_get(rv, "name")));
            hd_put(&b, "</td><td><code>");
            hd_esc(&b, yyjson_get_str(yyjson_obj_get(rv, "hub")));
            hd_put(&b, "</code></td>");
            hd_putf(&b, "<td class=\"num\">%lld</td><td class=\"num\">%lld</td>",
                    (long long)yyjson_get_int(yyjson_obj_get(rv, "members")),
                    (long long)yyjson_get_int(yyjson_obj_get(rv, "files")));
            double cohesion = yyjson_get_real(yyjson_obj_get(rv, "cohesion"));
            hd_putf(&b, "<td>%s (%.2f)</td></tr>\n", hd_cohesion_word(cohesion), cohesion);
        }
        hd_put(&b, "</table>\n");
        if (yyjson_arr_size(regions_arr) > shown)
            hd_putf(&b, "<p class=\"muted\">%zu of %zu regions shown — the rest are smaller.</p>\n",
                    shown, yyjson_arr_size(regions_arr));
    }

    /* Boundary spanners */
    yyjson_val *bridges_arr =
        bridges_doc ? yyjson_obj_get(yyjson_doc_get_root(bridges_doc), "bridges") : NULL;
    if (bridges_arr && yyjson_arr_size(bridges_arr) > 0) {
        hd_put(&b, "<h2>Boundary spanners — where regions meet</h2>\n"
                   "<p class=\"why\">These call into the most distinct regions. Changes here "
                   "ripple furthest; mention them when a task spans areas.</p>\n<table>\n"
                   "<tr><th>Symbol</th><th>File</th><th>Regions reached</th>"
                   "<th>Cross calls</th></tr>\n");
        for (size_t i = 0; i < yyjson_arr_size(bridges_arr); i++) {
            yyjson_val *rv = yyjson_arr_get(bridges_arr, i);
            hd_put(&b, "<tr><td><code>");
            hd_esc(&b, yyjson_get_str(yyjson_obj_get(rv, "name")));
            hd_put(&b, "</code></td><td>");
            hd_esc(&b, yyjson_get_str(yyjson_obj_get(rv, "file_path")));
            hd_putf(&b, "</td><td class=\"num\">%lld</td><td class=\"num\">%lld</td></tr>\n",
                    (long long)yyjson_get_int(yyjson_obj_get(rv, "regions")),
                    (long long)yyjson_get_int(yyjson_obj_get(rv, "cross_calls")));
        }
        hd_put(&b, "</table>\n");
    }

    /* Main journeys */
    if (flows_doc) {
        yyjson_val *froot = yyjson_doc_get_root(flows_doc);
        yyjson_val *flows_arr = yyjson_obj_get(froot, "flows");
        if (flows_arr && yyjson_arr_size(flows_arr) > 0) {
            hd_put(&b, "<h2>Main journeys — entry to effect</h2>\n"
                       "<p class=\"why\">Ranked walks from an entry point to a terminal "
                       "effect over resolved calls. Read one before touching the code it "
                       "crosses.</p>\n<table>\n<tr><th>Journey</th><th>Steps</th>"
                       "<th>Scope</th></tr>\n");
            size_t shown = 0;
            for (size_t i = 0; i < yyjson_arr_size(flows_arr) && shown < 10; i++, shown++) {
                yyjson_val *fv = yyjson_arr_get(flows_arr, i);
                hd_put(&b, "<tr><td><code>");
                hd_esc(&b, yyjson_get_str(yyjson_obj_get(fv, "label")));
                hd_putf(&b, "</code></td><td class=\"num\">%lld</td><td>%s</td></tr>\n",
                        (long long)yyjson_get_int(yyjson_obj_get(fv, "steps")),
                        yyjson_get_bool(yyjson_obj_get(fv, "cross_region")) ? "crosses regions"
                                                                            : "one region");
            }
            hd_put(&b, "</table>\n");
            long long callable_total = yyjson_get_int(yyjson_obj_get(froot, "callable_total"));
            hd_putf(&b,
                    "<p class=\"muted\">%zu of %zu detected journeys shown, walked from %lld "
                    "callables.</p>\n",
                    shown, yyjson_arr_size(flows_arr), callable_total);
        }
    }

    /* Risk — churn × complexity */
    if (metrics_doc) {
        yyjson_val *mroot = yyjson_doc_get_root(metrics_doc);
        bool churn_ok = yyjson_get_bool(yyjson_obj_get(mroot, "churn_available"));
        yyjson_val *risky = yyjson_obj_get(mroot, "top_churn_complex");
        if (churn_ok && risky && yyjson_arr_size(risky) > 0) {
            hd_put(&b, "<h2>Where change concentrates — churn × complexity</h2>\n"
                       "<p class=\"why\">Files that are both complicated and edited often: "
                       "the head of this list is where refactoring pays first.</p>\n"
                       "<table>\n<tr><th>File</th><th>Complexity</th><th>Commits (1y)</th>"
                       "</tr>\n");
            size_t shown = 0;
            for (size_t i = 0; i < yyjson_arr_size(risky) && shown < 10; i++, shown++) {
                yyjson_val *rv = yyjson_arr_get(risky, i);
                hd_put(&b, "<tr><td>");
                hd_esc(&b, yyjson_get_str(yyjson_obj_get(rv, "file")));
                hd_putf(&b, "</td><td class=\"num\">%lld</td><td class=\"num\">%lld</td></tr>\n",
                        (long long)yyjson_get_int(yyjson_obj_get(rv, "value")),
                        (long long)yyjson_get_int(yyjson_obj_get(rv, "commits")));
            }
            hd_put(&b, "</table>\n");
        } else if (!churn_ok) {
            hd_put(&b, "<h2>Where change concentrates</h2>\n<p class=\"muted\">No readable "
                       "git history at the indexed root, so churn cannot be computed — this "
                       "section would be guesswork and is omitted.</p>\n");
        }
    }

    /* Footer */
    {
        char stamp[64] = {0};
        time_t now = time(NULL);
        struct tm tm_utc;
#ifdef _WIN32
        gmtime_s(&tm_utc, &now);
#else
        gmtime_r(&now, &tm_utc);
#endif
        strftime(stamp, sizeof(stamp), "%Y-%m-%d %H:%M UTC", &tm_utc);
        hd_putf(&b,
                "<p class=\"muted\" style=\"margin-top:40px\">Generated %s by CBM Atlas from "
                "the indexed graph. Regenerate after re-indexing; do not edit by hand.</p>\n",
                stamp);
    }
    hd_put(&b, "</main>\n</body>\n</html>\n");

    if (regions_doc)
        yyjson_doc_free(regions_doc);
    if (metrics_doc)
        yyjson_doc_free(metrics_doc);
    if (flows_doc)
        yyjson_doc_free(flows_doc);
    if (bridges_doc)
        yyjson_doc_free(bridges_doc);
    free(regions_json);
    free(metrics_json);
    free(flows_json);
    free(bridges_json);

    if (b.oom) {
        free(b.data);
        return NULL;
    }
    return b.data;
}
