/*
 * callable_sig.h — signature-qualified callable identity (#2061).
 *
 * A callable's graph identity is
 *
 *     callable_qn := base_qn [<tparams>] (params) [cvref/async] [=>return_type]
 * Swift includes generic declarations and where requirements in <tparams>
 * (associated-type paths use '/' instead of '.') and appends function-level
 * async after params and an explicit return type after "=>". Swift preserves
 * qualified type paths using '/'. These distinguish declarations, not call
 * applicability.
 *
 * so overloads that share a base QN (Java/C#/C++/Kotlin/Swift/Scala/ObjC)
 * become distinct nodes instead of merging into one node that collects every
 * overload's CALLS edges. The `name` column stays the bare name.
 *
 * Every language selects a mode (cbm_callable_identity). A language whose
 * mode is CBM_CALLABLE_ID_NONE keeps its historical QN byte for byte; the
 * suffix is applied only where a mode is enabled, and the SAME builder
 * (cbm_callable_sig) serves the definition QN and the call-scope QN so the two
 * can never disagree.
 *
 * Suffix contract (what cbm_qn_callable_base_len inverts):
 *   - it never contains '.', "::" or "->" — the separators every QN leaf
 *     splitter uses — so a splitter that first strips the suffix finds the
 *     same leaf as before;
 *   - it is whitespace-normalized: one space only between two identifier
 *     characters ("unsigned long", "const T&"), none elsewhere;
 *   - type spelling: comments, annotations and attributes are dropped;
 *     except Swift, qualified paths keep their last segment (java.util.List
 *     -> List, std::vector -> vector); "->" in function types is spelled "=>"; a
 *     variadic/rest marker is "[]" for Java (the JVM array it is) and "~"
 *     everywhere else; C++ top-level cv-qualifiers are dropped and array
 *     parameters decay to pointers, as the language itself does;
 *   - it is at most CBM_CALLABLE_SIG_MAX bytes. A longer signature keeps the
 *     leading whole parameter entries that fit and ends in "#<16 hex>)", the
 *     FNV-1a 64 of the full uncapped suffix, so identity is preserved.
 */
#ifndef CBM_CALLABLE_SIG_H
#define CBM_CALLABLE_SIG_H

#include "cbm.h"
#include "arena.h"
#include "tree_sitter/api.h"
#include <stddef.h>
#include <stdint.h>

typedef enum {
    CBM_CALLABLE_ID_NONE = 0,      /* historical base QN, no suffix */
    CBM_CALLABLE_ID_TYPED,         /* (int,String)                          */
    CBM_CALLABLE_ID_LABELED_TYPED, /* (_:Data,to:URL)          Swift        */
    CBM_CALLABLE_ID_LABELED,       /* (_:style:)               ObjC selector */
    CBM_CALLABLE_ID_ARITY,         /* (2)                      dynamic langs */
} CBMCallableIdentity;

enum { CBM_CALLABLE_SIG_MAX = 200 };

/* The identity mode a language uses for callable QNs. Languages remain
 * CBM_CALLABLE_ID_NONE until their enable change lands with an index-format
 * bump; Swift is enabled by #2061. Kept as a side table, like
 * cbm_string_dispatch_suffixes, rather than a CBMLangSpec field: the ~160
 * positional lang_specs rows would all have to spell the new member under
 * -Wmissing-field-initializers. */
CBMCallableIdentity cbm_callable_identity(CBMLanguage lang);

/* Swift parameter defaults are call-resolution metadata, not graph identity.
 * Returns a bit per parameter; count > 64 is reported as 255. */
uint64_t cbm_swift_default_mask(TSNode func_node, const char *source, uint8_t *count);
/* The identity suffix for the callable at `func_node` in the language's own
 * mode, or NULL when that mode is NONE (or the node carries no parameter
 * information the mode can use). Arena-owned. */
const char *cbm_callable_sig(CBMArena *a, TSNode func_node, const char *source, CBMLanguage lang);

/* Same, with an explicit mode — the builder's golden tables exercise modes
 * independently of each language's production enablement. */
const char *cbm_callable_sig_mode(CBMArena *a, TSNode func_node, const char *source,
                                  CBMLanguage lang, CBMCallableIdentity mode);

/* Length of the base QN, i.e. `qn` without its callable identity suffix; equal
 * to strlen(qn) when there is none. Purely syntactic (the inverse of the
 * builder's contract above); a bare C++ `operator()` leaf is recognised as a
 * name, not a suffix. NULL -> 0. */
size_t cbm_qn_callable_base_len(const char *qn);

/* Top-level declaration return marker; strlen(suffix) if absent, 0 if malformed. */
size_t cbm_callable_return_offset(const char *suffix);

/* The inverse anchored to the callable's bare `name` (the node's name
 * column): the suffix is stripped only when the base QN is `name` or ends in
 * "." / "::" + `name`. Other QN text can end in ')' as well — a Java field
 * whose name is its declarator `X = f()`, a make `$(VAR)` target — and only
 * the anchor makes the inverse exact on it. Use this form wherever the name
 * is known; NULL name -> the unanchored form. */
size_t cbm_qn_callable_base_len_named(const char *qn, const char *name);

#endif /* CBM_CALLABLE_SIG_H */
