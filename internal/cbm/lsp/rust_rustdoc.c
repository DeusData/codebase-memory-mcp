/* rust_rustdoc.c — Parse rustdoc JSON and register into the type
 * registry. Pure-C JSON ingestion via the already-vendored yyjson.
 *
 * The rustdoc JSON shape (as of mid-2024):
 *
 *   { "format_version": <n>,
 *     "root": "0:0",
 *     "crate_version": "1.2.3",
 *     "includes_private": false,
 *     "index": {
 *       "<id>": {
 *         "id": "<id>",
 *         "name": "Foo",
 *         "visibility": "public",
 *         "inner": {
 *           "struct": {...} | "trait": {...} |
 *           "function": { "decl": { "inputs": [[name, type], ...],
 *                                   "output": <type> },
 *                         "header": {...} }
 *           | ...
 *         },
 *         "links": {...}
 *       }
 *     },
 *     "paths": { "<id>": { "path": ["serde", "Serialize"], "kind": "trait" } }
 *   }
 *
 * We walk `index`, look up the corresponding `paths[id]` to get the
 * fully-qualified path, and register types/methods/functions.
 *
 * The JSON format is unstable across versions; we tolerate missing
 * fields and skip items we can't make sense of (silently).
 */

#include "rust_rustdoc.h"
#include "type_rep.h"
#include "../../../vendored/yyjson/yyjson.h"
#include <string.h>
#include <stdio.h>

/* Concatenate a path array (yyjson_val of strings) into a single
 * dotted QN string, optionally prefixed by `crate_qn`. */
static const char* join_path(CBMArena* arena, yyjson_val* path_arr,
    const char* crate_qn) {
    if (!path_arr || !yyjson_is_arr(path_arr)) return NULL;
    char buf[1024];
    int off = 0;
    if (crate_qn && crate_qn[0]) {
        off = snprintf(buf, sizeof(buf), "%s", crate_qn);
    }
    yyjson_val* part;
    yyjson_arr_iter it;
    yyjson_arr_iter_init(path_arr, &it);
    bool first = true;
    while ((part = yyjson_arr_iter_next(&it))) {
        const char* s = yyjson_get_str(part);
        if (!s) continue;
        /* Skip the crate's own root segment if we already prefixed. */
        if (first && crate_qn && crate_qn[0] && strcmp(s, crate_qn) == 0) {
            first = false;
            continue;
        }
        if (off > 0 && off < (int)sizeof(buf) - 1) {
            buf[off++] = '.';
        }
        int n = snprintf(buf + off, sizeof(buf) - off, "%s", s);
        if (n < 0) break;
        off += n;
        if (off >= (int)sizeof(buf) - 1) break;
        first = false;
    }
    if (off <= 0) return NULL;
    return cbm_arena_strndup(arena, buf, (size_t)off);
}

/* Convert a rustdoc Type JSON value into a CBMType. Best-effort —
 * rustdoc's Type union is large. We map:
 *   - { "primitive": "i32" }      → BUILTIN
 *   - { "resolved_path": { path, args, ... } } → NAMED or TEMPLATE
 *   - { "borrowed_ref": { type, ... } }       → REFERENCE
 *   - { "slice": <T> }            → SLICE
 *   - { "array": { type, len }}   → SLICE
 *   - { "tuple": [<T>...] }       → TUPLE or BUILTIN("()") if empty
 *   - everything else             → UNKNOWN
 */
static const CBMType* parse_rustdoc_type(yyjson_val* t, CBMArena* arena,
    const char* crate_qn) {
    if (!t || !yyjson_is_obj(t)) return cbm_type_unknown();

    yyjson_val* prim = yyjson_obj_get(t, "primitive");
    if (prim && yyjson_is_str(prim)) {
        return cbm_type_builtin(arena, yyjson_get_str(prim));
    }
    yyjson_val* resolved = yyjson_obj_get(t, "resolved_path");
    if (resolved && yyjson_is_obj(resolved)) {
        const char* qn = NULL;
        yyjson_val* name = yyjson_obj_get(resolved, "name");
        yyjson_val* args = yyjson_obj_get(resolved, "args");
        if (name && yyjson_is_str(name)) {
            qn = cbm_arena_strdup(arena, yyjson_get_str(name));
        }
        if (!qn) return cbm_type_unknown();
        /* If `args.angle_bracketed.args` is non-empty, build TEMPLATE. */
        if (args && yyjson_is_obj(args)) {
            yyjson_val* angle = yyjson_obj_get(args, "angle_bracketed");
            if (angle && yyjson_is_obj(angle)) {
                yyjson_val* arr = yyjson_obj_get(angle, "args");
                if (arr && yyjson_is_arr(arr) && yyjson_arr_size(arr) > 0) {
                    const CBMType* targs[8];
                    int ti = 0;
                    yyjson_val* item;
                    yyjson_arr_iter it;
                    yyjson_arr_iter_init(arr, &it);
                    while ((item = yyjson_arr_iter_next(&it)) && ti < 8) {
                        yyjson_val* inner_t = yyjson_obj_get(item, "type");
                        if (inner_t) {
                            targs[ti++] = parse_rustdoc_type(inner_t, arena, crate_qn);
                        }
                    }
                    if (ti > 0) {
                        return cbm_type_template(arena, qn, targs, ti);
                    }
                }
            }
        }
        return cbm_type_named(arena, qn);
    }
    yyjson_val* borrowed = yyjson_obj_get(t, "borrowed_ref");
    if (borrowed && yyjson_is_obj(borrowed)) {
        yyjson_val* inner_t = yyjson_obj_get(borrowed, "type");
        return cbm_type_reference(arena,
            parse_rustdoc_type(inner_t, arena, crate_qn));
    }
    yyjson_val* slice = yyjson_obj_get(t, "slice");
    if (slice) {
        return cbm_type_slice(arena,
            parse_rustdoc_type(slice, arena, crate_qn));
    }
    yyjson_val* array = yyjson_obj_get(t, "array");
    if (array && yyjson_is_obj(array)) {
        yyjson_val* elem_t = yyjson_obj_get(array, "type");
        return cbm_type_slice(arena,
            parse_rustdoc_type(elem_t, arena, crate_qn));
    }
    yyjson_val* tuple = yyjson_obj_get(t, "tuple");
    if (tuple && yyjson_is_arr(tuple)) {
        size_t n = yyjson_arr_size(tuple);
        if (n == 0) return cbm_type_builtin(arena, "()");
        const CBMType* elems[8];
        int ei = 0;
        yyjson_val* item;
        yyjson_arr_iter it;
        yyjson_arr_iter_init(tuple, &it);
        while ((item = yyjson_arr_iter_next(&it)) && ei < 8) {
            elems[ei++] = parse_rustdoc_type(item, arena, crate_qn);
        }
        if (ei == 1) return elems[0];
        return cbm_type_tuple(arena, elems, ei);
    }
    return cbm_type_unknown();
}

/* Register a function item: figure out the receiver QN (if any), the
 * short name, and the return type. */
static bool register_function(CBMTypeRegistry* reg, CBMArena* arena,
    const char* short_name, const char* full_qn, const char* receiver,
    yyjson_val* function_obj, const char* crate_qn) {
    if (!short_name || !full_qn) return false;
    CBMRegisteredFunc rf;
    memset(&rf, 0, sizeof(rf));
    rf.qualified_name = full_qn;
    rf.short_name = short_name;
    rf.receiver_type = receiver;
    rf.min_params = -1;

    /* Return type — rustdoc's `function.decl.output` may be omitted
     * (implicit unit). */
    const CBMType* ret = cbm_type_builtin(arena, "()");
    if (function_obj && yyjson_is_obj(function_obj)) {
        yyjson_val* decl = yyjson_obj_get(function_obj, "decl");
        if (decl && yyjson_is_obj(decl)) {
            yyjson_val* out = yyjson_obj_get(decl, "output");
            if (out && !yyjson_is_null(out)) {
                ret = parse_rustdoc_type(out, arena, crate_qn);
            }
        }
    }
    const CBMType** ra = (const CBMType**)cbm_arena_alloc(arena,
        2 * sizeof(const CBMType*));
    ra[0] = ret;
    ra[1] = NULL;
    rf.signature = cbm_type_func(arena, NULL, NULL, ra);
    cbm_registry_add_func(reg, rf);
    return true;
}

int cbm_rust_rustdoc_ingest(CBMTypeRegistry* reg, CBMArena* arena,
    const char* json, int json_len, const char* crate_qn) {
    if (!reg || !arena || !json) return 0;
    yyjson_doc* doc = yyjson_read(json, (size_t)(json_len > 0 ? json_len : strlen(json)), 0);
    if (!doc) return 0;
    yyjson_val* root = yyjson_doc_get_root(doc);
    if (!yyjson_is_obj(root)) { yyjson_doc_free(doc); return 0; }

    yyjson_val* index = yyjson_obj_get(root, "index");
    yyjson_val* paths = yyjson_obj_get(root, "paths");
    if (!index || !paths) { yyjson_doc_free(doc); return 0; }

    int count = 0;
    yyjson_obj_iter it;
    yyjson_obj_iter_init(index, &it);
    yyjson_val* key;
    while ((key = yyjson_obj_iter_next(&it))) {
        const char* item_id = yyjson_get_str(key);
        if (!item_id) continue;
        yyjson_val* item = yyjson_obj_iter_get_val(key);
        if (!item || !yyjson_is_obj(item)) continue;

        yyjson_val* name = yyjson_obj_get(item, "name");
        const char* short_name = yyjson_get_str(name);
        if (!short_name) continue;

        yyjson_val* inner = yyjson_obj_get(item, "inner");
        if (!inner || !yyjson_is_obj(inner)) continue;

        /* Look up the fully-qualified path. */
        yyjson_val* path_entry = yyjson_obj_get(paths, item_id);
        const char* qn = NULL;
        const char* kind = NULL;
        if (path_entry && yyjson_is_obj(path_entry)) {
            qn = join_path(arena, yyjson_obj_get(path_entry, "path"), crate_qn);
            yyjson_val* k = yyjson_obj_get(path_entry, "kind");
            kind = yyjson_get_str(k);
        }
        if (!qn) {
            /* Methods/impl-items don't have entries in `paths`; we still
             * register them by name + receiver derived from the parent. */
        }

        /* Dispatch on the rustdoc kind. */
        if (kind && (strcmp(kind, "struct") == 0 ||
                     strcmp(kind, "enum") == 0 ||
                     strcmp(kind, "union") == 0)) {
            CBMRegisteredType rt;
            memset(&rt, 0, sizeof(rt));
            rt.qualified_name = qn;
            rt.short_name = cbm_arena_strdup(arena, short_name);
            cbm_registry_add_type(reg, rt);
            count++;
        } else if (kind && strcmp(kind, "trait") == 0) {
            CBMRegisteredType rt;
            memset(&rt, 0, sizeof(rt));
            rt.qualified_name = qn;
            rt.short_name = cbm_arena_strdup(arena, short_name);
            rt.is_interface = true;
            cbm_registry_add_type(reg, rt);
            count++;
        } else if (kind && strcmp(kind, "function") == 0) {
            yyjson_val* fn = yyjson_obj_get(inner, "function");
            if (register_function(reg, arena, cbm_arena_strdup(arena, short_name),
                                  qn, NULL, fn, crate_qn)) {
                count++;
            }
        }
        /* Impl items / methods don't have path entries; they're
         * referenced from struct/trait items' `impls` list. A complete
         * walker would chase those — for now we keep ingestion bounded
         * to the top-level paths. */
    }
    yyjson_doc_free(doc);
    return count;
}
