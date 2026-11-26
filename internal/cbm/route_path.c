/*
 * route_path.c -- one Route QN per endpoint, whoever names it: a client call
 * site, a handler's decorator or an OpenAPI operation (moved here from the
 * route pass so extraction can name a Route as the pipeline does).
 */
#include "cbm.h"

#include <stdbool.h>
#include <stddef.h>

/* True for characters that may appear in a ":name" route parameter. */
static inline bool is_route_ident_char(char c) {
    return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_';
}

/* Canonicalize route-path parameter placeholders to a single "{}" token so that
 * client call sites and server handlers rendezvous on the same Route QN
 * regardless of framework syntax. Each parameter token collapses to "{}":
 *
 *   :name    Express / React-Router / Rails / typical JS API clients
 *   {name}   Axum / Spring / OpenAPI / ASP.NET
 *   <name>   Flask / Rocket (incl. typed "<int:id>")
 *   ${...}   JS template interpolation captured into the path
 *
 * Parameter names are intentionally discarded so the same logical endpoint
 * matches across services that name the path variable differently. Static path
 * text is copied verbatim; the result never exceeds the input length. */
const char *cbm_route_canon_path(const char *in, char *out, size_t out_sz) {
    if (out == NULL || out_sz == 0) {
        /* WHY: cppcheck value-flow follows a caller that passes an as-yet
         * uninitialized output buffer (e.g. `char cpath[256];`) and flags this
         * `return out` as uninitvar. False positive: we only return the pointer
         * here and never read the buffer; returning it (NULL, or a buffer too
         * small to write into) is the intended early-out contract. */
        // cppcheck-suppress uninitvar
        return out;
    }
    if (in == NULL) {
        out[0] = '\0';
        return out;
    }
    const size_t last = out_sz - 1;
    size_t oi = 0;
    size_t i = 0;
    while (in[i] != '\0' && oi < last) {
        char c = in[i];
        bool at_seg_start = (oi == 0) || (out[oi - 1] == '/');
        bool is_param = false;

        if (c == ':' && at_seg_start && is_route_ident_char(in[i + 1])) {
            i++;
            while (in[i] != '\0' && is_route_ident_char(in[i])) {
                i++;
            }
            is_param = true;
        } else if (c == '{') {
            i++;
            while (in[i] != '\0' && in[i] != '}' && in[i] != '/') {
                i++;
            }
            if (in[i] == '}') {
                i++;
            }
            is_param = true;
        } else if (c == '<') {
            i++;
            while (in[i] != '\0' && in[i] != '>' && in[i] != '/') {
                i++;
            }
            if (in[i] == '>') {
                i++;
            }
            is_param = true;
        } else if (c == '$' && in[i + 1] == '{') {
            i += 2;
            while (in[i] != '\0' && in[i] != '}' && in[i] != '/') {
                i++;
            }
            if (in[i] == '}') {
                i++;
            }
            is_param = true;
        }

        if (is_param) {
            if (oi + 2 > last) {
                break;
            }
            out[oi++] = '{';
            out[oi++] = '}';
            continue;
        }
        out[oi++] = c;
        i++;
    }
    out[oi] = '\0';
    return out;
}
