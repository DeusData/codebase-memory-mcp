#ifndef CBM_EXTRACT_URL_FOLD_H
#define CBM_EXTRACT_URL_FOLD_H

/* URL constant folding for HTTP client calls (issues #706, #1147).
 *
 * A caller that centralises its endpoints writes the URL as an expression over
 * module constants -- `API_URL = API_BASE_URL + "some/path"` in Python,
 * `${BASE}/games/launch` or `API_ENDPOINTS.USER` in JS/TS. These helpers fold
 * such an expression into one string: string literals and module-level string
 * constants (including members of an object-literal constant) inline, and a
 * part that cannot be resolved becomes the canonical "{}" placeholder. Python
 * and the JS/TS family only. */

#include "cbm.h"
#include "tree_sitter/api.h"

/* True for the languages whose expressions are folded. */
bool cbm_url_fold_lang(CBMLanguage lang);

/* True when a URL carries something literal besides '/' and "{}"
 * placeholders, i.e. a real path to recover rather than only unknowns. */
bool cbm_url_has_literal_path(const char *url);

/* Record a module-level `name = value` in the per-file string constant map
 * with separate raw and URL-projected values. An object-literal value records each string-valued
 * member as "name.KEY" (nested objects as "name.A.B"). */
void cbm_url_fold_record_constant(CBMExtractCtx *ctx, const char *name, TSNode value);

/* The URL a client call receives through `arg`, or NULL when the argument is
 * not a foldable expression or holds nothing literal. A base that cannot be
 * resolved is dropped and the literal path after it kept ("{}/foo" -> "/foo",
 * "{}some/path" -> "/some/path"), exactly like #1249 / #2291: the path is
 * partial, never presented with an invented base. */
const char *cbm_url_fold_call_url(CBMExtractCtx *ctx, TSNode arg);

/* The raw fold, including unresolved placeholders. Never changes separators
 * or removes a leading unknown component; suitable for topics and generic calls. */
const char *cbm_url_fold_call_raw(CBMExtractCtx *ctx, TSNode arg);

/* The raw resolved value of a foldable, non-literal argument, or NULL when
 * any leading part stayed unresolved. For slots that must hold an exact
 * value (CBMCallArg.value). */
const char *cbm_url_fold_exact(CBMExtractCtx *ctx, TSNode arg);

#endif /* CBM_EXTRACT_URL_FOLD_H */
