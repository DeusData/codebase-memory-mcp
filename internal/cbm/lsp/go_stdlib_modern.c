// go_stdlib_modern.c — hand-maintained addendum to the generated Go stdlib
// table (generated/go_stdlib_data.c). The generated table's 34-package
// allowlist predates Go 1.21 and its generator (scripts/gen-go-stdlib.go) no
// longer exists in the repo, so the modern generic surface is registered here
// by hand: slices, maps, cmp, iter, math/rand/v2, unique, weak, structs and
// testing/synctest (stable in 1.25).
//
// Unlike the generated table, entries here set type_param_names plus
// CBM_TYPE_TYPE_PARAM parameter/return reps so the EXISTING implicit-generics
// unifier (go_unify_type / the substitution consumers in go_eval_expr_type)
// infers concrete returns: `us := slices.Clone(users)` gives us the []User
// element type, so `us[0].Name()` dispatches.
//
// Iterator-returning functions (slices.Values/All/Sorted/Collect,
// maps.Keys/Values/All/Collect, iter.Pull/Pull2) are registered with
// STRUCTURAL func-shaped returns — func(yield func(V) bool) — not the nominal
// iter.Seq spelling: CBMRegisteredType has no underlying-rep field, so a
// nominal return would be an opaque NAMED type the range binder could never
// see through. iter.Seq/Seq2 are additionally registered as named types so
// `iter.Seq[T]` spellings in project signatures resolve to a known type.
//
// Deliberately NOT here (对拍 adjudication):
//   * sync.OnceFunc/OnceValue/OnceValues — already in the generated table;
//     re-registering would create duplicate QNs with unspecified lookup
//     preference.
//   * upgrades of the flattened `any` iterator returns inside the existing 34
//     packages (strings.SplitSeq/FieldsSeq/Lines, bytes equivalents): those
//     require patching or regenerating the generated entries, not appending —
//     tracked separately.
//
// Called immediately after cbm_go_stdlib_register at every registry-build
// site and BEFORE go_mark_stdlib_types, so every type added here carries
// is_stdlib and can never ambiguate a project interface in the
// sole-implementer scan.

#include "type_rep.h"
#include "type_registry.h"
#include "go_lsp.h"
#include <string.h>

/* ── tiny constructors (arena-allocated, NULL-terminated vectors) ── */

static const CBMType **gsm_types1(CBMArena *a, const CBMType *t0) {
    const CBMType **v = (const CBMType **)cbm_arena_alloc(a, 2 * sizeof(*v));
    v[0] = t0;
    v[1] = NULL;
    return v;
}

static const CBMType **gsm_types2(CBMArena *a, const CBMType *t0, const CBMType *t1) {
    const CBMType **v = (const CBMType **)cbm_arena_alloc(a, 3 * sizeof(*v));
    v[0] = t0;
    v[1] = t1;
    v[2] = NULL;
    return v;
}

static const CBMType **gsm_types3(CBMArena *a, const CBMType *t0, const CBMType *t1,
                                  const CBMType *t2) {
    const CBMType **v = (const CBMType **)cbm_arena_alloc(a, 4 * sizeof(*v));
    v[0] = t0;
    v[1] = t1;
    v[2] = t2;
    v[3] = NULL;
    return v;
}

static const char **gsm_names1(CBMArena *a, const char *n0) {
    const char **v = (const char **)cbm_arena_alloc(a, 2 * sizeof(*v));
    v[0] = n0;
    v[1] = NULL;
    return v;
}

static const char **gsm_names2(CBMArena *a, const char *n0, const char *n1) {
    const char **v = (const char **)cbm_arena_alloc(a, 3 * sizeof(*v));
    v[0] = n0;
    v[1] = n1;
    v[2] = NULL;
    return v;
}

/* func(yield func(V) bool) — the structural shape of iter.Seq[V]. */
static const CBMType *gsm_seq(CBMArena *a, const CBMType *v) {
    const CBMType *yield =
        cbm_type_func(a, NULL, gsm_types1(a, v), gsm_types1(a, cbm_type_builtin(a, "bool")));
    return cbm_type_func(a, NULL, gsm_types1(a, yield), NULL);
}

/* func(yield func(K, V) bool) — the structural shape of iter.Seq2[K, V]. */
static const CBMType *gsm_seq2(CBMArena *a, const CBMType *k, const CBMType *v) {
    const CBMType *yield =
        cbm_type_func(a, NULL, gsm_types2(a, k, v), gsm_types1(a, cbm_type_builtin(a, "bool")));
    return cbm_type_func(a, NULL, gsm_types1(a, yield), NULL);
}

/* Register one function. tparams may be NULL for non-generic entries. */
static void gsm_func(CBMTypeRegistry *reg, CBMArena *a, const char *qn, const char *sn,
                     const char *recv, const char **tparams, const CBMType **params,
                     const CBMType **rets) {
    CBMRegisteredFunc rf;
    memset(&rf, 0, sizeof(rf));
    rf.qualified_name = qn;
    rf.short_name = sn;
    rf.receiver_type = recv;
    rf.type_param_names = tparams;
    rf.signature = cbm_type_func(a, NULL, params, rets);
    cbm_registry_add_func(reg, rf);
}

static void gsm_type(CBMTypeRegistry *reg, const char *qn, const char *sn, const char **tparams,
                     const char **method_names, bool is_interface) {
    CBMRegisteredType rt;
    memset(&rt, 0, sizeof(rt));
    rt.qualified_name = qn;
    rt.short_name = sn;
    rt.type_param_names = tparams;
    rt.method_names = method_names;
    rt.is_interface = is_interface;
    cbm_registry_add_type(reg, rt);
}

/* ── package registrars ─────────────────────────────────────────── */

static void gsm_register_slices(CBMTypeRegistry *reg, CBMArena *a) {
    const char **tpE = gsm_names1(a, "E");
    const CBMType *E = cbm_type_type_param(a, "E");
    const CBMType *slE = cbm_type_slice(a, E);
    const CBMType *tint = cbm_type_builtin(a, "int");
    const CBMType *tbool = cbm_type_builtin(a, "bool");
    const CBMType *cmp2 = cbm_type_func(a, NULL, gsm_types2(a, E, E), gsm_types1(a, tint));
    const CBMType *eq2 = cbm_type_func(a, NULL, gsm_types2(a, E, E), gsm_types1(a, tbool));
    const CBMType *pred = cbm_type_func(a, NULL, gsm_types1(a, E), gsm_types1(a, tbool));
    const CBMType *seqE = gsm_seq(a, E);
    const CBMType *seq2iE = gsm_seq2(a, tint, E);

#define GSM_SL(name, params, rets) \
    gsm_func(reg, a, "slices." name, name, NULL, tpE, (params), (rets))

    GSM_SL("Contains", gsm_types2(a, slE, E), gsm_types1(a, tbool));
    GSM_SL("ContainsFunc", gsm_types2(a, slE, pred), gsm_types1(a, tbool));
    GSM_SL("Index", gsm_types2(a, slE, E), gsm_types1(a, tint));
    GSM_SL("IndexFunc", gsm_types2(a, slE, pred), gsm_types1(a, tint));
    GSM_SL("BinarySearch", gsm_types2(a, slE, E), gsm_types2(a, tint, tbool));
    GSM_SL("BinarySearchFunc", gsm_types3(a, slE, E, cmp2), gsm_types2(a, tint, tbool));
    GSM_SL("Sort", gsm_types1(a, slE), NULL);
    GSM_SL("SortFunc", gsm_types2(a, slE, cmp2), NULL);
    GSM_SL("SortStableFunc", gsm_types2(a, slE, cmp2), NULL);
    GSM_SL("IsSorted", gsm_types1(a, slE), gsm_types1(a, tbool));
    GSM_SL("IsSortedFunc", gsm_types2(a, slE, cmp2), gsm_types1(a, tbool));
    GSM_SL("Sorted", gsm_types1(a, seqE), gsm_types1(a, slE));
    GSM_SL("SortedFunc", gsm_types2(a, seqE, cmp2), gsm_types1(a, slE));
    GSM_SL("SortedStableFunc", gsm_types2(a, seqE, cmp2), gsm_types1(a, slE));
    GSM_SL("Min", gsm_types1(a, slE), gsm_types1(a, E));
    GSM_SL("MinFunc", gsm_types2(a, slE, cmp2), gsm_types1(a, E));
    GSM_SL("Max", gsm_types1(a, slE), gsm_types1(a, E));
    GSM_SL("MaxFunc", gsm_types2(a, slE, cmp2), gsm_types1(a, E));
    GSM_SL("Clone", gsm_types1(a, slE), gsm_types1(a, slE));
    GSM_SL("Compact", gsm_types1(a, slE), gsm_types1(a, slE));
    GSM_SL("CompactFunc", gsm_types2(a, slE, eq2), gsm_types1(a, slE));
    GSM_SL("Compare", gsm_types2(a, slE, slE), gsm_types1(a, tint));
    GSM_SL("CompareFunc", gsm_types3(a, slE, slE, cmp2), gsm_types1(a, tint));
    GSM_SL("Equal", gsm_types2(a, slE, slE), gsm_types1(a, tbool));
    GSM_SL("EqualFunc", gsm_types3(a, slE, slE, eq2), gsm_types1(a, tbool));
    GSM_SL("Delete", gsm_types3(a, slE, tint, tint), gsm_types1(a, slE));
    GSM_SL("DeleteFunc", gsm_types2(a, slE, pred), gsm_types1(a, slE));
    GSM_SL("Insert", gsm_types3(a, slE, tint, E), gsm_types1(a, slE));
    GSM_SL("Replace", gsm_types3(a, slE, tint, tint), gsm_types1(a, slE));
    GSM_SL("Grow", gsm_types2(a, slE, tint), gsm_types1(a, slE));
    GSM_SL("Clip", gsm_types1(a, slE), gsm_types1(a, slE));
    GSM_SL("Reverse", gsm_types1(a, slE), NULL);
    GSM_SL("Concat", gsm_types1(a, cbm_type_slice(a, slE)), gsm_types1(a, slE));
    GSM_SL("Repeat", gsm_types2(a, slE, tint), gsm_types1(a, slE));
    /* 1.23 iterator surface — structural func-shaped returns. */
    GSM_SL("Values", gsm_types1(a, slE), gsm_types1(a, seqE));
    GSM_SL("All", gsm_types1(a, slE), gsm_types1(a, seq2iE));
    GSM_SL("Backward", gsm_types1(a, slE), gsm_types1(a, seq2iE));
    GSM_SL("Collect", gsm_types1(a, seqE), gsm_types1(a, slE));
    GSM_SL("AppendSeq", gsm_types2(a, slE, seqE), gsm_types1(a, slE));
    GSM_SL("Chunk", gsm_types2(a, slE, tint), gsm_types1(a, gsm_seq(a, slE)));
#undef GSM_SL
}

static void gsm_register_maps(CBMTypeRegistry *reg, CBMArena *a) {
    const char **tpKV = gsm_names2(a, "K", "V");
    const CBMType *K = cbm_type_type_param(a, "K");
    const CBMType *V = cbm_type_type_param(a, "V");
    const CBMType *mKV = cbm_type_map(a, K, V);
    const CBMType *tbool = cbm_type_builtin(a, "bool");

#define GSM_MP(name, params, rets) \
    gsm_func(reg, a, "maps." name, name, NULL, tpKV, (params), (rets))

    GSM_MP("Clone", gsm_types1(a, mKV), gsm_types1(a, mKV));
    GSM_MP("Copy", gsm_types2(a, mKV, mKV), NULL);
    GSM_MP("DeleteFunc",
           gsm_types2(a, mKV,
                      cbm_type_func(a, NULL, gsm_types2(a, K, V), gsm_types1(a, tbool))),
           NULL);
    GSM_MP("Equal", gsm_types2(a, mKV, mKV), gsm_types1(a, tbool));
    GSM_MP("EqualFunc", gsm_types2(a, mKV, mKV), gsm_types1(a, tbool));
    /* 1.23 iterator surface — structural func-shaped returns. */
    GSM_MP("Keys", gsm_types1(a, mKV), gsm_types1(a, gsm_seq(a, K)));
    GSM_MP("Values", gsm_types1(a, mKV), gsm_types1(a, gsm_seq(a, V)));
    GSM_MP("All", gsm_types1(a, mKV), gsm_types1(a, gsm_seq2(a, K, V)));
    GSM_MP("Collect", gsm_types1(a, gsm_seq2(a, K, V)), gsm_types1(a, mKV));
    GSM_MP("Insert", gsm_types2(a, mKV, gsm_seq2(a, K, V)), NULL);
#undef GSM_MP
}

static void gsm_register_cmp(CBMTypeRegistry *reg, CBMArena *a) {
    const char **tpT = gsm_names1(a, "T");
    const CBMType *T = cbm_type_type_param(a, "T");
    gsm_func(reg, a, "cmp.Compare", "Compare", NULL, tpT, gsm_types2(a, T, T),
             gsm_types1(a, cbm_type_builtin(a, "int")));
    gsm_func(reg, a, "cmp.Less", "Less", NULL, tpT, gsm_types2(a, T, T),
             gsm_types1(a, cbm_type_builtin(a, "bool")));
    gsm_func(reg, a, "cmp.Or", "Or", NULL, tpT, gsm_types1(a, T), gsm_types1(a, T));
    /* cmp.Ordered is a constraint interface — registered as a named type so
     * project signatures spelling it resolve. */
    gsm_type(reg, "cmp.Ordered", "Ordered", NULL, NULL, true);
}

static void gsm_register_iter(CBMTypeRegistry *reg, CBMArena *a) {
    /* Named types for project signatures spelling iter.Seq[T]; the structural
     * shape lives on the FUNCTIONS that return iterators (see header). */
    gsm_type(reg, "iter.Seq", "Seq", gsm_names1(a, "V"), NULL, false);
    gsm_type(reg, "iter.Seq2", "Seq2", gsm_names2(a, "K", "V"), NULL, false);

    const char **tpV = gsm_names1(a, "V");
    const char **tpKV = gsm_names2(a, "K", "V");
    const CBMType *V = cbm_type_type_param(a, "V");
    const CBMType *K = cbm_type_type_param(a, "K");
    const CBMType *tbool = cbm_type_builtin(a, "bool");
    /* iter.Pull(Seq[V]) → (next func() (V, bool), stop func()) */
    const CBMType *nextV = cbm_type_func(a, NULL, NULL, gsm_types2(a, V, tbool));
    const CBMType *stop = cbm_type_func(a, NULL, NULL, NULL);
    gsm_func(reg, a, "iter.Pull", "Pull", NULL, tpV, gsm_types1(a, gsm_seq(a, V)),
             gsm_types2(a, nextV, stop));
    const CBMType *nextKV = cbm_type_func(a, NULL, NULL, gsm_types3(a, K, V, tbool));
    gsm_func(reg, a, "iter.Pull2", "Pull2", NULL, tpKV, gsm_types1(a, gsm_seq2(a, K, V)),
             gsm_types2(a, nextKV, stop));
}

static void gsm_register_rand_v2(CBMTypeRegistry *reg, CBMArena *a) {
    const char *pkg = "math/rand/v2";
    const CBMType *tint = cbm_type_builtin(a, "int");
    const CBMType *ti32 = cbm_type_builtin(a, "int32");
    const CBMType *ti64 = cbm_type_builtin(a, "int64");
    const CBMType *tu = cbm_type_builtin(a, "uint");
    const CBMType *tu32 = cbm_type_builtin(a, "uint32");
    const CBMType *tu64 = cbm_type_builtin(a, "uint64");
    const CBMType *tf32 = cbm_type_builtin(a, "float32");
    const CBMType *tf64 = cbm_type_builtin(a, "float64");
    const CBMType *slint = cbm_type_slice(a, tint);
    const CBMType *randT = cbm_type_named(a, "math/rand/v2.Rand");
    const CBMType *randP = cbm_type_pointer(a, randT);
    const CBMType *srcT = cbm_type_named(a, "math/rand/v2.Source");

    gsm_type(reg, "math/rand/v2.Rand", "Rand", NULL, NULL, false);
    gsm_type(reg, "math/rand/v2.PCG", "PCG", NULL, NULL, false);
    gsm_type(reg, "math/rand/v2.ChaCha8", "ChaCha8", NULL, NULL, false);
    gsm_type(reg, "math/rand/v2.Zipf", "Zipf", NULL, NULL, false);
    gsm_type(reg, "math/rand/v2.Source", "Source", NULL, gsm_names1(a, "Uint64"), true);

    /* Top-level funcs + Rand methods share names/shapes; emit both. */
    struct {
        const char *name;
        const CBMType **params;
        const CBMType **rets;
    } fns[] = {
        {"Int", NULL, gsm_types1(a, tint)},
        {"Int32", NULL, gsm_types1(a, ti32)},
        {"Int64", NULL, gsm_types1(a, ti64)},
        {"IntN", gsm_types1(a, tint), gsm_types1(a, tint)},
        {"Int32N", gsm_types1(a, ti32), gsm_types1(a, ti32)},
        {"Int64N", gsm_types1(a, ti64), gsm_types1(a, ti64)},
        {"Uint", NULL, gsm_types1(a, tu)},
        {"Uint32", NULL, gsm_types1(a, tu32)},
        {"Uint64", NULL, gsm_types1(a, tu64)},
        {"UintN", gsm_types1(a, tu), gsm_types1(a, tu)},
        {"Uint32N", gsm_types1(a, tu32), gsm_types1(a, tu32)},
        {"Uint64N", gsm_types1(a, tu64), gsm_types1(a, tu64)},
        {"Float32", NULL, gsm_types1(a, tf32)},
        {"Float64", NULL, gsm_types1(a, tf64)},
        {"ExpFloat64", NULL, gsm_types1(a, tf64)},
        {"NormFloat64", NULL, gsm_types1(a, tf64)},
        {"Perm", gsm_types1(a, tint), gsm_types1(a, slint)},
        {"Shuffle", gsm_types1(a, tint), NULL},
        {NULL, NULL, NULL},
    };
    for (int i = 0; fns[i].name; i++) {
        gsm_func(reg, a, cbm_arena_sprintf(a, "%s.%s", pkg, fns[i].name), fns[i].name, NULL, NULL,
                 fns[i].params, fns[i].rets);
        gsm_func(reg, a, cbm_arena_sprintf(a, "%s.Rand.%s", pkg, fns[i].name), fns[i].name,
                 "math/rand/v2.Rand", NULL, fns[i].params, fns[i].rets);
    }
    /* Generic rand.N (1.22). */
    {
        const char **tpN = gsm_names1(a, "Int");
        const CBMType *N = cbm_type_type_param(a, "Int");
        gsm_func(reg, a, "math/rand/v2.N", "N", NULL, tpN, gsm_types1(a, N), gsm_types1(a, N));
    }
    gsm_func(reg, a, "math/rand/v2.New", "New", NULL, NULL, gsm_types1(a, srcT),
             gsm_types1(a, randP));
    gsm_func(reg, a, "math/rand/v2.NewPCG", "NewPCG", NULL, NULL, gsm_types2(a, tu64, tu64),
             gsm_types1(a, cbm_type_pointer(a, cbm_type_named(a, "math/rand/v2.PCG"))));
    gsm_func(reg, a, "math/rand/v2.NewChaCha8", "NewChaCha8", NULL, NULL, NULL,
             gsm_types1(a, cbm_type_pointer(a, cbm_type_named(a, "math/rand/v2.ChaCha8"))));
    gsm_func(reg, a, "math/rand/v2.NewZipf", "NewZipf", NULL, NULL, NULL,
             gsm_types1(a, cbm_type_pointer(a, cbm_type_named(a, "math/rand/v2.Zipf"))));
    gsm_func(reg, a, "math/rand/v2.Zipf.Uint64", "Uint64", "math/rand/v2.Zipf", NULL, NULL,
             gsm_types1(a, tu64));
    gsm_func(reg, a, "math/rand/v2.PCG.Uint64", "Uint64", "math/rand/v2.PCG", NULL, NULL,
             gsm_types1(a, tu64));
    gsm_func(reg, a, "math/rand/v2.PCG.Seed", "Seed", "math/rand/v2.PCG", NULL,
             gsm_types2(a, tu64, tu64), NULL);
    gsm_func(reg, a, "math/rand/v2.ChaCha8.Uint64", "Uint64", "math/rand/v2.ChaCha8", NULL, NULL,
             gsm_types1(a, tu64));
}

static void gsm_register_unique_weak_structs(CBMTypeRegistry *reg, CBMArena *a) {
    const char **tpT = gsm_names1(a, "T");
    const CBMType *T = cbm_type_type_param(a, "T");

    /* unique (1.23): Make[T](T) Handle[T]; Handle.Value() T. The nominal
     * Handle return keeps the method set reachable (unique.Handle.Value). */
    gsm_type(reg, "unique.Handle", "Handle", tpT, NULL, false);
    gsm_func(reg, a, "unique.Make", "Make", NULL, tpT, gsm_types1(a, T),
             gsm_types1(a, cbm_type_named(a, "unique.Handle")));
    gsm_func(reg, a, "unique.Handle.Value", "Value", "unique.Handle", tpT, NULL, gsm_types1(a, T));

    /* weak (1.24): Make[T](*T) Pointer[T]; Pointer.Value() *T. */
    gsm_type(reg, "weak.Pointer", "Pointer", tpT, NULL, false);
    gsm_func(reg, a, "weak.Make", "Make", NULL, tpT, gsm_types1(a, cbm_type_pointer(a, T)),
             gsm_types1(a, cbm_type_named(a, "weak.Pointer")));
    gsm_func(reg, a, "weak.Pointer.Value", "Value", "weak.Pointer", tpT, NULL,
             gsm_types1(a, cbm_type_pointer(a, T)));

    /* structs (1.23): HostLayout marker. */
    gsm_type(reg, "structs.HostLayout", "HostLayout", NULL, NULL, false);
}

static void gsm_register_synctest(CBMTypeRegistry *reg, CBMArena *a) {
    /* testing/synctest — stable in Go 1.25: Test(t, f) runs f in a bubble;
     * Wait() blocks until the bubble is durably idle. */
    const CBMType *tT = cbm_type_pointer(a, cbm_type_named(a, "testing.T"));
    const CBMType *fn = cbm_type_func(a, NULL, gsm_types1(a, tT), NULL);
    gsm_func(reg, a, "testing/synctest.Test", "Test", NULL, NULL, gsm_types2(a, tT, fn), NULL);
    gsm_func(reg, a, "testing/synctest.Wait", "Wait", NULL, NULL, NULL, NULL);
}

void cbm_go_stdlib_register_modern(CBMTypeRegistry *reg, CBMArena *arena) {
    if (!reg || !arena) {
        return;
    }
    gsm_register_slices(reg, arena);
    gsm_register_maps(reg, arena);
    gsm_register_cmp(reg, arena);
    gsm_register_iter(reg, arena);
    gsm_register_rand_v2(reg, arena);
    gsm_register_unique_weak_structs(reg, arena);
    gsm_register_synctest(reg, arena);
}
