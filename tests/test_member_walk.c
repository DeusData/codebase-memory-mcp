/*
 * test_member_walk.c — the shared member lookup (lsp/member_walk.h).
 *
 * Lifted from the parked Ruby branch (its five tests, commit 96547561c) and
 * corrected: the END of an order is "unknown", only an explicit STOP is
 * "absent". A toy language hands over each type's order as text:
 *   O:X own members of X     S:X X's whole order (a sub-order)
 *   Q:X own members of X, written outside the type's body (Ruby)
 *   ?   unseen (hard)        ~   unseen, soft (passed) L:~ soft unknown, phase 2
 *   !   stop                 P   open (__call)
 *   N:X X's order goes on    L:X X searched after the chain   L:? unknown there
 *   G:n a group of n         X:n an exclusive group of n (traits)
 * Own members come from a real registry. Work is counted, never timed.
 */
#include "test_framework.h"
#include "lsp/member_walk.h"
#include "lsp/type_registry.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct {
    const char *name;
    const char *order;
} mwt_type_t;

typedef struct {
    const mwt_type_t *types;
    int count;
} mwt_lang_t;

static mwt_lang_t g_mwt_lang;

static bool mwt_lin(CBMMemberOrders *o, const CBMRegisteredType *t, int side, CBMMemberSteps *out) {
    for (int i = 0; i < g_mwt_lang.count; i++) {
        if (strcmp(g_mwt_lang.types[i].name, t->qualified_name) != 0) {
            continue;
        }
        const char *p = g_mwt_lang.types[i].order;
        while (*p) {
            while (*p == ' ') {
                p++;
            }
            if (!*p) {
                break;
            }
            const char *end = strchr(p, ' ');
            size_t n = end ? (size_t)(end - p) : strlen(p);
            const char *arg = n > 2 ? cbm_arena_strndup(o->arena, p + 2, n - 2) : NULL;
            if (n == 1 && p[0] == '?') {
                cbm_member_steps_push(out, CBM_MW_UNSEEN, NULL, side, CBM_MW_PLACE_OWN);
            } else if (n == 1 && p[0] == '~') {
                cbm_member_steps_soft(out, CBM_MW_UNSEEN, side, CBM_MW_PLACE_OWN);
            } else if (n == 3 && p[0] == 'L' && p[2] == '~') {
                cbm_member_steps_soft(out, CBM_MW_LATER, side, CBM_MW_PLACE_INTERFACE_DEFAULT);
            } else if (n == 1 && p[0] == '!') {
                cbm_member_steps_push(out, CBM_MW_STOP, NULL, side, CBM_MW_PLACE_OWN);
            } else if (n == 1 && p[0] == 'P') {
                cbm_member_steps_push(out, CBM_MW_OPEN, NULL, side, CBM_MW_PLACE_OWN);
            } else if (arg && p[0] == 'O') {
                cbm_member_steps_push(out, CBM_MW_OWN, arg, side, CBM_MW_PLACE_OWN);
            } else if (arg && p[0] == 'Q') {
                cbm_member_steps_push(out, CBM_MW_OWN, arg, side, CBM_MW_PLACE_OUTSIDE);
            } else if (arg && p[0] == 'S') {
                cbm_member_steps_push(out, CBM_MW_SUB, arg, side, CBM_MW_PLACE_MIXIN);
            } else if (arg && p[0] == 'N') {
                cbm_member_steps_push(out, CBM_MW_NEXT, arg, side, CBM_MW_PLACE_SUPER);
            } else if (arg && p[0] == 'L') {
                cbm_member_steps_push(out, CBM_MW_LATER, strcmp(arg, "?") == 0 ? NULL : arg, side,
                                      CBM_MW_PLACE_INTERFACE_DEFAULT);
            } else if (arg && (p[0] == 'G' || p[0] == 'X')) {
                cbm_member_steps_group(out, t->qualified_name, atoi(arg),
                                       p[0] == 'X' ? CBM_MW_GROUP_EXCLUSIVE : 0);
            }
            p += n;
        }
        return true;
    }
    return false;
}

typedef struct {
    CBMArena arena;
    CBMTypeRegistry reg;
    CBMMemberOrders orders;
    CBMMemberWalker walker;
} mwt_fixture_t;

static void mwt_open(mwt_fixture_t *f, const mwt_type_t *types, int count) {
    cbm_arena_init(&f->arena);
    cbm_registry_init(&f->reg, &f->arena);
    g_mwt_lang.types = types;
    g_mwt_lang.count = count;
    for (int i = 0; i < count; i++) {
        CBMRegisteredType rt;
        memset(&rt, 0, sizeof(rt));
        rt.qualified_name = types[i].name;
        rt.short_name = types[i].name;
        cbm_registry_add_type(&f->reg, rt);
    }
    cbm_member_orders_init(&f->orders, &f->reg, mwt_lin, &f->arena);
    cbm_member_walker_init(&f->walker, &f->orders);
}

static void mwt_close(mwt_fixture_t *f) {
    cbm_member_walker_free(&f->walker);
    cbm_member_orders_free(&f->orders);
    cbm_arena_destroy(&f->arena);
}

static void mwt_add_method(CBMTypeRegistry *reg, const char *type, const char *name) {
    CBMRegisteredFunc rf;
    memset(&rf, 0, sizeof(rf));
    rf.min_params = -1;
    rf.receiver_type = type;
    rf.short_name = name;
    rf.qualified_name = cbm_arena_sprintf(reg->arena, "%s.%s", type, name);
    cbm_registry_add_func(reg, rf);
}

static CBMMemberWalkResult mwt_find(mwt_fixture_t *f, const char *type, const char *name,
                                   const char *after, CBMMemberHit *hit) {
    CBMMemberQuery q;
    memset(&q, 0, sizeof(q));
    q.reg = &f->reg;
    q.type_qn = type;
    q.name = name;
    q.after = after;
    return cbm_member_lookup(&f->walker, &q, hit);
}

static CBMMemberWalkResult mwt_walk(mwt_fixture_t *f, const char *type, const char *name,
                                   const char *after, const char **hit_qn) {
    CBMMemberHit hit;
    CBMMemberWalkResult r = mwt_find(f, type, name, after, &hit);
    *hit_qn = hit.func ? hit.func->qualified_name : NULL;
    return r;
}

TEST(member_walk_searches_the_given_order) {
    /* C: prepend P, own, include I2 then I1, superclass S, then an unseen
     * ancestor. D: own, an unseen step, S. E: own, stop, S (never reached).
     * G's order goes on as H's, H's as S2's; a link anywhere but last is
     * malformed. */
    static const mwt_type_t types[] = {
        {"C", "O:P O:C O:I2 O:I1 O:S ?"},
        {"D", "O:D ? O:S"},
        {"E", "O:E ! O:S"},
        {"G", "O:G O:GM N:H"},
        {"H", "O:H N:S2"},
        {"S2", "O:S2 !"},
        {"BadLink", "N:H O:G"},
    };
    mwt_fixture_t f;
    mwt_open(&f, types, (int)(sizeof(types) / sizeof(types[0])));
    mwt_add_method(&f.reg, "P", "shadowed");
    mwt_add_method(&f.reg, "C", "shadowed");
    mwt_add_method(&f.reg, "C", "own");
    mwt_add_method(&f.reg, "I1", "both");
    mwt_add_method(&f.reg, "I2", "both");
    mwt_add_method(&f.reg, "S", "inherited");
    mwt_add_method(&f.reg, "D", "dee");
    mwt_add_method(&f.reg, "S2", "deep");
    mwt_add_method(&f.reg, "H", "kay");
    cbm_registry_finalize(&f.reg);

    const char *qn = NULL;
    ASSERT_EQ(mwt_walk(&f, "C", "shadowed", NULL, &qn), CBM_MW_FOUND);
    ASSERT_STR_EQ(qn, "P.shadowed"); /* the order puts the prepend first */
    ASSERT_EQ(mwt_walk(&f, "C", "both", NULL, &qn), CBM_MW_FOUND);
    ASSERT_STR_EQ(qn, "I2.both");
    ASSERT_EQ(mwt_walk(&f, "C", "inherited", NULL, &qn), CBM_MW_FOUND);
    ASSERT_STR_EQ(qn, "S.inherited");
    /* Past every visible type into the unseen one: no answer. */
    ASSERT_EQ(mwt_walk(&f, "C", "nobody", NULL, &qn), CBM_MW_ABSTAIN);
    ASSERT_NULL(qn);
    /* Before an unseen step a hit stands; behind it nothing answers. */
    ASSERT_EQ(mwt_walk(&f, "D", "dee", NULL, &qn), CBM_MW_FOUND);
    ASSERT_EQ(mwt_walk(&f, "D", "inherited", NULL, &qn), CBM_MW_ABSTAIN);
    /* A stop ends the search: absent, and S behind it never answers. */
    ASSERT_EQ(mwt_walk(&f, "E", "inherited", NULL, &qn), CBM_MW_ABSENT);
    /* `super` from C's own definition starts after C. */
    ASSERT_EQ(mwt_walk(&f, "C", "both", "C", &qn), CBM_MW_FOUND);
    ASSERT_STR_EQ(qn, "I2.both");
    ASSERT_EQ(mwt_walk(&f, "C", "shadowed", "C", &qn), CBM_MW_ABSTAIN);
    /* A `super` start that is not in the order: abstain. */
    ASSERT_EQ(mwt_walk(&f, "C", "both", "Elsewhere", &qn), CBM_MW_ABSTAIN);
    /* Linked orders read as one: G, GM, H, S2, then the stop. */
    ASSERT_EQ(mwt_walk(&f, "G", "deep", NULL, &qn), CBM_MW_FOUND);
    ASSERT_STR_EQ(qn, "S2.deep");
    ASSERT_EQ(mwt_walk(&f, "G", "nobody", NULL, &qn), CBM_MW_ABSENT);
    ASSERT_EQ(mwt_walk(&f, "G", "kay", "H", &qn), CBM_MW_ABSENT); /* super past H */
    ASSERT_EQ(mwt_walk(&f, "G", "kay", NULL, &qn), CBM_MW_FOUND);
    ASSERT_EQ(mwt_walk(&f, "BadLink", "nobody", NULL, &qn), CBM_MW_ABSTAIN);
    mwt_close(&f);
    PASS();
}

/* The correction that matters most: an order a builder forgot to end with
 * STOP (or with an UNSEEN) answers "unknown", never "not found". The
 * specimen returned ABSENT here. Partner: the same order with STOP. */
TEST(member_walk_end_without_stop_is_unknown) {
    static const mwt_type_t types[] = {
        {"F", "O:F O:K O:S O:K"},
        {"FStop", "O:F O:K O:S O:K !"},
        {"Linked", "O:L N:F"},
        {"LinkedStop", "O:L N:FStop"},
    };
    mwt_fixture_t f;
    mwt_open(&f, types, 4);
    mwt_add_method(&f.reg, "K", "kay");
    cbm_registry_finalize(&f.reg);
    const char *qn = NULL;
    ASSERT_EQ(mwt_walk(&f, "F", "nobody", NULL, &qn), CBM_MW_ABSTAIN);
    ASSERT_EQ(mwt_walk(&f, "FStop", "nobody", NULL, &qn), CBM_MW_ABSENT);
    ASSERT_EQ(mwt_walk(&f, "Linked", "nobody", NULL, &qn), CBM_MW_ABSTAIN);
    ASSERT_EQ(mwt_walk(&f, "LinkedStop", "nobody", NULL, &qn), CBM_MW_ABSENT);
    /* A hit before the end stands in both. */
    ASSERT_EQ(mwt_walk(&f, "F", "kay", NULL, &qn), CBM_MW_FOUND);
    ASSERT_EQ(mwt_walk(&f, "Linked", "kay", NULL, &qn), CBM_MW_FOUND);
    /* The zero value of a result is "unknown". */
    CBMMemberWalkResult zero;
    memset(&zero, 0, sizeof(zero));
    ASSERT_EQ(zero, CBM_MW_ABSTAIN);
    mwt_close(&f);
    PASS();
}

TEST(member_walk_ambiguous_own_member_abstains) {
    static const mwt_type_t types[] = {
        {"R", "O:R O:M !"},
        {"M", "O:M !"},
    };
    mwt_fixture_t f;
    mwt_open(&f, types, 2);
    /* Two definitions answer for R.dup (a class reopened in two files). */
    CBMRegisteredFunc rf;
    memset(&rf, 0, sizeof(rf));
    rf.min_params = -1;
    rf.receiver_type = "R";
    rf.short_name = "dup";
    rf.qualified_name = "a.R.dup";
    cbm_registry_add_func(&f.reg, rf);
    rf.qualified_name = "b.R.dup";
    cbm_registry_add_func(&f.reg, rf);
    /* The same definition registered twice is one answer. */
    rf.qualified_name = "a.R.once";
    rf.short_name = "once";
    cbm_registry_add_func(&f.reg, rf);
    cbm_registry_add_func(&f.reg, rf);
    mwt_add_method(&f.reg, "M", "dup");
    cbm_registry_finalize(&f.reg);
    const char *qn = NULL;
    ASSERT_EQ(mwt_walk(&f, "R", "dup", NULL, &qn), CBM_MW_ABSTAIN);
    ASSERT_NULL(qn);
    ASSERT_EQ(mwt_walk(&f, "R", "once", NULL, &qn), CBM_MW_FOUND);
    ASSERT_STR_EQ(qn, "a.R.once");
    mwt_close(&f);
    PASS();
}

/* An order of n own steps; a miss reads every one of them. */
static uint64_t mwt_order_work(int n) {
    size_t cap = (size_t)n * 12 + 16;
    char *order = (char *)calloc(cap, 1);
    if (!order) {
        return 0;
    }
    size_t used = 0;
    for (int i = 0; i < n; i++) {
        used += (size_t)snprintf(order + used, cap - used, "O:M%d ", i);
    }
    snprintf(order + used, cap - used, "!");
    mwt_type_t types[] = {{"Root", order}};
    mwt_fixture_t f;
    mwt_open(&f, types, 1);
    cbm_registry_finalize(&f.reg);
    const char *qn = NULL;
    uint64_t before = f.walker.work;
    CBMMemberWalkResult r = mwt_walk(&f, "Root", "nobody", NULL, &qn);
    uint64_t work = r == CBM_MW_ABSENT ? f.walker.work - before : 0;
    mwt_close(&f);
    free(order);
    return work;
}

TEST(member_walk_work_is_one_per_step) {
    uint64_t w_n = mwt_order_work(200);
    uint64_t w_2n = mwt_order_work(400);
    printf("  member walk work: n=200 %llu, n=400 %llu\n", (unsigned long long)w_n,
           (unsigned long long)w_2n);
    ASSERT_EQ(w_n, 201); /* n own steps and the stop */
    ASSERT_EQ(w_2n, 401);
    PASS();
}

/* A group: P first, then {C, B} in an order nobody knows, then S. */
TEST(member_walk_group_answers_once_or_abstains) {
    static const mwt_type_t types[] = {
        {"K", "O:P G:2 O:C O:B O:S !"},
        {"Bad", "O:P G:3 O:C"},
    };
    mwt_fixture_t f;
    mwt_open(&f, types, 2);
    mwt_add_method(&f.reg, "C", "c_only");
    mwt_add_method(&f.reg, "B", "b_only");
    mwt_add_method(&f.reg, "C", "both");
    mwt_add_method(&f.reg, "B", "both");
    mwt_add_method(&f.reg, "S", "both");
    mwt_add_method(&f.reg, "S", "s_only");
    mwt_add_method(&f.reg, "P", "p_first");
    mwt_add_method(&f.reg, "C", "p_first");
    cbm_registry_finalize(&f.reg);
    const char *qn = NULL;
    ASSERT_EQ(mwt_walk(&f, "K", "c_only", NULL, &qn), CBM_MW_FOUND);
    ASSERT_STR_EQ(qn, "C.c_only");
    ASSERT_EQ(mwt_walk(&f, "K", "b_only", NULL, &qn), CBM_MW_FOUND);
    ASSERT_STR_EQ(qn, "B.b_only");
    /* Two members answer: their order decides, nobody knows it. */
    ASSERT_EQ(mwt_walk(&f, "K", "both", NULL, &qn), CBM_MW_ABSTAIN);
    ASSERT_NULL(qn);
    /* None answers: the walk goes on after the group. */
    ASSERT_EQ(mwt_walk(&f, "K", "s_only", NULL, &qn), CBM_MW_FOUND);
    ASSERT_STR_EQ(qn, "S.s_only");
    ASSERT_EQ(mwt_walk(&f, "K", "nobody", NULL, &qn), CBM_MW_ABSENT);
    /* A step before the group still wins. */
    ASSERT_EQ(mwt_walk(&f, "K", "p_first", NULL, &qn), CBM_MW_FOUND);
    ASSERT_STR_EQ(qn, "P.p_first");
    /* `super` from a member of the group: abstain; from before it: the
     * group answers as usual. */
    ASSERT_EQ(mwt_walk(&f, "K", "s_only", "C", &qn), CBM_MW_ABSTAIN);
    ASSERT_EQ(mwt_walk(&f, "K", "c_only", "P", &qn), CBM_MW_FOUND);
    ASSERT_STR_EQ(qn, "C.c_only");
    /* A group longer than the order is malformed. */
    ASSERT_EQ(mwt_walk(&f, "Bad", "c_only", NULL, &qn), CBM_MW_ABSTAIN);
    mwt_close(&f);
    PASS();
}

/* An exclusive group (PHP traits): a valid program has one provider, so a
 * hit stands beside an unknown member; a plain group needs every member
 * known. Partners in the same fixture. */
TEST(member_walk_exclusive_group_with_unknown_member) {
    static const mwt_type_t types[] = {
        {"Excl", "O:Excl X:2 O:T ? O:S !"},
        {"Plain", "O:Plain G:2 O:T ? O:S !"},
    };
    mwt_fixture_t f;
    mwt_open(&f, types, 2);
    mwt_add_method(&f.reg, "T", "tm");
    mwt_add_method(&f.reg, "S", "sm");
    cbm_registry_finalize(&f.reg);
    const char *qn = NULL;
    ASSERT_EQ(mwt_walk(&f, "Excl", "tm", NULL, &qn), CBM_MW_FOUND);
    ASSERT_STR_EQ(qn, "T.tm");
    ASSERT_EQ(mwt_walk(&f, "Plain", "tm", NULL, &qn), CBM_MW_ABSTAIN);
    /* Nobody in the group answers but one member is unknown: unknown in both. */
    ASSERT_EQ(mwt_walk(&f, "Excl", "sm", NULL, &qn), CBM_MW_ABSTAIN);
    ASSERT_EQ(mwt_walk(&f, "Plain", "sm", NULL, &qn), CBM_MW_ABSTAIN);
    mwt_close(&f);
    PASS();
}

/* Where a hit lies, and whether the steps before it were all known. */
TEST(member_walk_hit_reports_place_and_completeness) {
    static const mwt_type_t types[] = {
        {"Kid", "O:Kid X:1 S:Tr L:Ifc N:Base"},
        {"Base", "O:Base !"},
        {"Tr", "O:Tr X:1 S:Inner !"},
        {"Inner", "O:Inner !"},
        {"Ifc", "O:Ifc L:IfcParent !"},
        {"IfcParent", "O:IfcParent !"},
        {"Out", "O:Out Q:Ext O:Last !"},
    };
    mwt_fixture_t f;
    mwt_open(&f, types, 7);
    mwt_add_method(&f.reg, "Kid", "own");
    mwt_add_method(&f.reg, "Base", "sup");
    mwt_add_method(&f.reg, "Tr", "mix");
    mwt_add_method(&f.reg, "Inner", "deepmix");
    mwt_add_method(&f.reg, "Ifc", "decl");
    mwt_add_method(&f.reg, "IfcParent", "pdecl");
    mwt_add_method(&f.reg, "Base", "decl"); /* the class chain answers before interfaces */
    mwt_add_method(&f.reg, "Ext", "ext");
    mwt_add_method(&f.reg, "Last", "last");
    cbm_registry_finalize(&f.reg);
    CBMMemberHit h;
    ASSERT_EQ(mwt_find(&f, "Kid", "own", NULL, &h), CBM_MW_FOUND);
    ASSERT_EQ(h.place, CBM_MW_PLACE_OWN);
    ASSERT_STR_EQ(h.owner_qn, "Kid");
    ASSERT_EQ(mwt_find(&f, "Kid", "sup", NULL, &h), CBM_MW_FOUND);
    ASSERT_EQ(h.place, CBM_MW_PLACE_SUPER);
    ASSERT_STR_EQ(h.owner_qn, "Base");
    ASSERT_EQ(mwt_find(&f, "Kid", "mix", NULL, &h), CBM_MW_FOUND);
    ASSERT_EQ(h.place, CBM_MW_PLACE_MIXIN);
    ASSERT_STR_EQ(h.owner_qn, "Tr");
    ASSERT_STR_EQ(h.via_qn, "Kid");
    /* A trait inside a trait: still reported through the outermost user. */
    ASSERT_EQ(mwt_find(&f, "Kid", "deepmix", NULL, &h), CBM_MW_FOUND);
    ASSERT_EQ(h.place, CBM_MW_PLACE_MIXIN);
    ASSERT_STR_EQ(h.owner_qn, "Inner");
    ASSERT_STR_EQ(h.via_qn, "Kid");
    ASSERT_EQ(mwt_find(&f, "Kid", "decl", NULL, &h), CBM_MW_FOUND);
    ASSERT_STR_EQ(h.owner_qn, "Base");
    ASSERT_EQ(mwt_find(&f, "Kid", "pdecl", NULL, &h), CBM_MW_FOUND);
    ASSERT_EQ(h.place, CBM_MW_PLACE_INTERFACE_DEFAULT);
    ASSERT_STR_EQ(h.owner_qn, "IfcParent");
    ASSERT_TRUE(h.prefix_complete);
    ASSERT_EQ(mwt_find(&f, "Kid", "nobody", NULL, &h), CBM_MW_ABSENT);
    /* A step written outside the body: a hit there says so; a hit after it
     * did not see a complete prefix. */
    ASSERT_EQ(mwt_find(&f, "Out", "ext", NULL, &h), CBM_MW_FOUND);
    ASSERT_EQ(h.place, CBM_MW_PLACE_OUTSIDE);
    ASSERT_EQ(mwt_find(&f, "Out", "last", NULL, &h), CBM_MW_FOUND);
    ASSERT_FALSE(h.prefix_complete);
    ASSERT_EQ(mwt_find(&f, "Kid", "own", NULL, &h), CBM_MW_FOUND);
    ASSERT_TRUE(h.prefix_complete);
    mwt_close(&f);
    PASS();
}

/* OPEN (__call, method_missing): what nobody declares is unknown, never
 * absent; declared members still answer, before and after it. */
TEST(member_walk_open_turns_absent_into_unknown) {
    static const mwt_type_t types[] = {
        {"Magic", "O:Magic P N:Base"},
        {"Base", "O:Base L:Ifc !"},
        {"Ifc", "O:Ifc !"},
        {"Plain", "O:Plain N:Base"},
    };
    mwt_fixture_t f;
    mwt_open(&f, types, 4);
    mwt_add_method(&f.reg, "Magic", "__call");
    mwt_add_method(&f.reg, "Base", "inherited");
    mwt_add_method(&f.reg, "Ifc", "declared");
    cbm_registry_finalize(&f.reg);
    const char *qn = NULL;
    ASSERT_EQ(mwt_walk(&f, "Magic", "anything", NULL, &qn), CBM_MW_ABSTAIN);
    ASSERT_EQ(mwt_walk(&f, "Plain", "anything", NULL, &qn), CBM_MW_ABSENT);
    ASSERT_EQ(mwt_walk(&f, "Magic", "inherited", NULL, &qn), CBM_MW_FOUND);
    ASSERT_STR_EQ(qn, "Base.inherited");
    ASSERT_EQ(mwt_walk(&f, "Magic", "declared", NULL, &qn), CBM_MW_FOUND);
    ASSERT_EQ(mwt_walk(&f, "Magic", "__call", NULL, &qn), CBM_MW_FOUND);
    mwt_close(&f);
    PASS();
}

/* Phase 2 searches only after a complete chain: an unseen superclass hides
 * the interfaces (the implementation may be there); an unknown interface
 * answers unknown when it is reached. */
TEST(member_walk_later_needs_a_complete_chain) {
    static const mwt_type_t types[] = {
        {"Vend", "O:Vend L:Ifc ?"},
        {"Proj", "O:Proj L:Ifc !"},
        {"Two", "O:Two L:? L:Ifc !"},
        {"TwoB", "O:TwoB L:Ifc L:? !"},
        {"Ifc", "O:Ifc !"},
    };
    mwt_fixture_t f;
    mwt_open(&f, types, 5);
    mwt_add_method(&f.reg, "Ifc", "decl");
    cbm_registry_finalize(&f.reg);
    const char *qn = NULL;
    ASSERT_EQ(mwt_walk(&f, "Vend", "decl", NULL, &qn), CBM_MW_ABSTAIN);
    ASSERT_EQ(mwt_walk(&f, "Proj", "decl", NULL, &qn), CBM_MW_FOUND);
    ASSERT_EQ(mwt_walk(&f, "Two", "decl", NULL, &qn), CBM_MW_ABSTAIN);
    ASSERT_EQ(mwt_walk(&f, "TwoB", "decl", NULL, &qn), CBM_MW_FOUND);
    ASSERT_EQ(mwt_walk(&f, "TwoB", "nobody", NULL, &qn), CBM_MW_ABSTAIN);
    ASSERT_EQ(mwt_walk(&f, "Proj", "nobody", NULL, &qn), CBM_MW_ABSENT);
    mwt_close(&f);
    PASS();
}

/* Renames a type applies to its mixins: PHP `T1::m insteadof T2;
 * T2::m as m2;` and `x as y` with the trait left out. */
TEST(member_walk_renames_alias_and_exclude) {
    static const mwt_type_t types[] = {
        {"User", "O:User X:2 S:T1 S:T2 !"},
        {"NoRules", "O:NoRules X:2 S:T1 S:T2 !"},
        {"T1", "O:T1 !"},
        {"T2", "O:T2 !"},
    };
    mwt_fixture_t f;
    mwt_open(&f, types, 4);
    static const CBMMemberRename renames[] = {
        {"m", "T2", NULL, CBM_RENAME_EXCLUDE},
        {"m2", "T2", "m", CBM_RENAME_ALIAS},
        {"only1", NULL, "one", CBM_RENAME_ALIAS},
        {"ghost", "Unseen", "m", CBM_RENAME_ALIAS}, /* a trait that is not a member */
    };
    for (int i = 0; i < f.reg.type_count; i++) {
        if (strcmp(f.reg.types[i].qualified_name, "User") == 0) {
            f.reg.types[i].renames = renames;
            f.reg.types[i].rename_count = 4;
        }
    }
    mwt_add_method(&f.reg, "T1", "m");
    mwt_add_method(&f.reg, "T2", "m");
    mwt_add_method(&f.reg, "T1", "one");
    cbm_registry_finalize(&f.reg);
    CBMMemberHit h;
    ASSERT_EQ(mwt_find(&f, "User", "m", NULL, &h), CBM_MW_FOUND);
    ASSERT_STR_EQ(h.func->qualified_name, "T1.m");
    ASSERT_STR_EQ(h.via_qn, "User");
    ASSERT_EQ(mwt_find(&f, "User", "m2", NULL, &h), CBM_MW_FOUND);
    ASSERT_STR_EQ(h.func->qualified_name, "T2.m");
    ASSERT_STR_EQ(h.member_name, "m");
    ASSERT_EQ(mwt_find(&f, "User", "only1", NULL, &h), CBM_MW_FOUND);
    ASSERT_STR_EQ(h.func->qualified_name, "T1.one");
    /* An alias of a trait the group does not hold: unknown, never absent. */
    ASSERT_EQ(mwt_find(&f, "User", "ghost", NULL, &h), CBM_MW_ABSTAIN);
    /* Partner: without the rules the two providers of m conflict. */
    ASSERT_EQ(mwt_find(&f, "NoRules", "m", NULL, &h), CBM_MW_ABSTAIN);
    ASSERT_EQ(mwt_find(&f, "NoRules", "m2", NULL, &h), CBM_MW_ABSENT);
    mwt_close(&f);
    PASS();
}

/* Cycles abstain; a diamond is searched once and is one answer. */
TEST(member_walk_cycles_abstain_diamonds_agree) {
    static const mwt_type_t types[] = {
        {"A", "O:A N:B"},
        {"B", "O:B N:A"},
        {"User", "O:User X:2 S:Ping S:Aud !"},
        {"Ping", "O:Ping X:1 S:Pong !"},
        {"Pong", "O:Pong X:1 S:Ping !"},
        {"Aud", "O:Aud !"},
        {"Dia", "O:Dia X:2 S:L S:R !"},
        {"L", "O:L X:1 S:Base !"},
        {"R", "O:R X:1 S:Base !"},
        {"Base", "O:Base !"},
    };
    mwt_fixture_t f;
    mwt_open(&f, types, 10);
    mwt_add_method(&f.reg, "B", "bee");
    mwt_add_method(&f.reg, "Aud", "tag");
    mwt_add_method(&f.reg, "Pong", "pong");
    mwt_add_method(&f.reg, "Base", "shared");
    cbm_registry_finalize(&f.reg);
    const char *qn = NULL;
    ASSERT_EQ(mwt_walk(&f, "A", "bee", NULL, &qn), CBM_MW_FOUND); /* before the cycle closes */
    ASSERT_EQ(mwt_walk(&f, "A", "nobody", NULL, &qn), CBM_MW_ABSTAIN);
    ASSERT_EQ(mwt_walk(&f, "User", "tag", NULL, &qn), CBM_MW_FOUND);
    ASSERT_STR_EQ(qn, "Aud.tag");
    ASSERT_EQ(mwt_walk(&f, "User", "pong", NULL, &qn), CBM_MW_FOUND);
    ASSERT_EQ(mwt_walk(&f, "User", "nobody", NULL, &qn), CBM_MW_ABSTAIN);
    ASSERT_EQ(mwt_walk(&f, "Dia", "shared", NULL, &qn), CBM_MW_FOUND);
    ASSERT_STR_EQ(qn, "Base.shared");
    ASSERT_EQ(mwt_walk(&f, "Dia", "nobody", NULL, &qn), CBM_MW_ABSENT);
    mwt_close(&f);
    PASS();
}

/* Fields are members too; ANY prefers a method. */
TEST(member_walk_fields) {
    static const mwt_type_t types[] = {
        {"Kid", "O:Kid N:Base"},
        {"Base", "O:Base !"},
    };
    mwt_fixture_t f;
    mwt_open(&f, types, 2);
    static const char *names[] = {"count", "both", NULL};
    const CBMType *ftypes[3];
    ftypes[0] = cbm_type_builtin(&f.arena, "int");
    ftypes[1] = cbm_type_builtin(&f.arena, "string");
    ftypes[2] = NULL;
    for (int i = 0; i < f.reg.type_count; i++) {
        if (strcmp(f.reg.types[i].qualified_name, "Base") == 0) {
            f.reg.types[i].field_names = names;
            f.reg.types[i].field_types = ftypes;
        }
    }
    mwt_add_method(&f.reg, "Base", "both");
    cbm_registry_finalize(&f.reg);
    CBMMemberQuery q;
    memset(&q, 0, sizeof(q));
    q.reg = &f.reg;
    q.type_qn = "Kid";
    q.name = "count";
    q.member_kind = CBM_MW_MEMBER_FIELD;
    CBMMemberHit h;
    ASSERT_EQ(cbm_member_lookup(&f.walker, &q, &h), CBM_MW_FOUND);
    ASSERT_EQ(h.field_type, ftypes[0]);
    ASSERT_STR_EQ(h.owner_qn, "Base");
    q.member_kind = CBM_MW_MEMBER_METHOD;
    ASSERT_EQ(cbm_member_lookup(&f.walker, &q, &h), CBM_MW_ABSENT);
    q.name = "both";
    q.member_kind = CBM_MW_MEMBER_ANY;
    ASSERT_EQ(cbm_member_lookup(&f.walker, &q, &h), CBM_MW_FOUND);
    ASSERT_NOT_NULL(h.func);
    mwt_close(&f);
    PASS();
}

/* COLLECT gathers every declaration; HIDE_BY_SIGNATURE lets a derived one
 * hide a base one of equal signature. An unknown place leaves the set
 * incomplete: unknown. */
TEST(member_walk_collect_modes) {
    static const mwt_type_t types[] = {
        {"Kid", "O:Kid N:Base"},
        {"Base", "O:Base !"},
        {"Vend", "O:Vend ?"},
    };
    mwt_fixture_t f;
    mwt_open(&f, types, 3);
    const CBMType *p_int[2] = {cbm_type_builtin(&f.arena, "int"), NULL};
    const CBMType *p_str[2] = {cbm_type_builtin(&f.arena, "string"), NULL};
    const char *owners[3] = {"Kid", "Base", "Base"};
    const CBMType **params[3] = {p_int, p_int, p_str};
    for (int i = 0; i < 3; i++) {
        CBMRegisteredFunc rf;
        memset(&rf, 0, sizeof(rf));
        rf.min_params = -1;
        rf.receiver_type = owners[i];
        rf.short_name = "put";
        rf.qualified_name = cbm_arena_sprintf(&f.arena, "%s.put#%d", owners[i], i);
        rf.signature = cbm_type_func(&f.arena, NULL, params[i], NULL);
        cbm_registry_add_func(&f.reg, rf);
    }
    mwt_add_method(&f.reg, "Vend", "put");
    cbm_registry_finalize(&f.reg);
    CBMMemberQuery q;
    memset(&q, 0, sizeof(q));
    q.reg = &f.reg;
    q.type_qn = "Kid";
    q.name = "put";
    q.overloads = CBM_MW_COLLECT;
    CBMMemberHit h;
    ASSERT_EQ(cbm_member_lookup(&f.walker, &q, &h), CBM_MW_FOUND);
    ASSERT_EQ(f.walker.hit_count, 3);
    q.overloads = CBM_MW_HIDE_BY_SIGNATURE;
    ASSERT_EQ(cbm_member_lookup(&f.walker, &q, &h), CBM_MW_FOUND);
    ASSERT_EQ(f.walker.hit_count, 2); /* Kid.put(int) hides Base.put(int) */
    ASSERT_STR_EQ(f.walker.hits[0].owner_qn, "Kid");
    q.type_qn = "Vend";
    ASSERT_EQ(cbm_member_lookup(&f.walker, &q, &h), CBM_MW_ABSTAIN);
    q.overloads = CBM_MW_HIDE_BY_NAME;
    ASSERT_EQ(cbm_member_lookup(&f.walker, &q, &h), CBM_MW_FOUND); /* hide by name: first wins */
    mwt_close(&f);
    PASS();
}

/* A soft unknown place (a supertype outside the project) is passed: a hit
 * behind it is a possible answer (prefix incomplete), and the lookup never
 * ends absent. Partners: the same orders with a hard unknown abstain; a hit
 * before the place is complete. */
TEST(member_walk_soft_unknown_places) {
    static const mwt_type_t types[] = {
        {"Soft", "O:Soft ~ O:Behind !"},
        {"Hard", "O:Hard ? O:Behind !"},
        {"SoftGroup", "O:SoftGroup X:2 S:Tr ~ O:Behind !"},
        {"SoftFirst", "O:SoftFirst X:2 ~ S:Tr O:Behind !"},
        {"SoftPlain", "O:SoftPlain G:2 S:Tr ~ !"},
        {"SoftLater", "O:SoftLater L:~ L:Ifc !"},
        {"LaterSoft", "O:LaterSoft L:Ifc L:~ !"},
        {"HardLater", "O:HardLater L:? L:Ifc !"},
        {"ToGhost", "O:ToGhost L:Ifc N:Ghost"},
        {"Tr", "O:Tr !"},
        {"Ifc", "O:Ifc !"},
    };
    mwt_fixture_t f;
    mwt_open(&f, types, 11);
    mwt_add_method(&f.reg, "Soft", "first");
    mwt_add_method(&f.reg, "Behind", "late");
    mwt_add_method(&f.reg, "Tr", "trm");
    mwt_add_method(&f.reg, "Ifc", "decl");
    mwt_add_method(&f.reg, "Ghost", "gm"); /* a receiver only, no registered type */
    cbm_registry_finalize(&f.reg);
    CBMMemberHit h;
    /* Before the place: proven; behind it: possible; never absent. */
    ASSERT_EQ(mwt_find(&f, "Soft", "first", NULL, &h), CBM_MW_FOUND);
    ASSERT_TRUE(h.prefix_complete);
    ASSERT_EQ(mwt_find(&f, "Soft", "late", NULL, &h), CBM_MW_FOUND);
    ASSERT_FALSE(h.prefix_complete);
    ASSERT_EQ(mwt_find(&f, "Soft", "nobody", NULL, &h), CBM_MW_ABSTAIN);
    /* Partner: a hard unknown place ends the walk. */
    ASSERT_EQ(mwt_find(&f, "Hard", "late", NULL, &h), CBM_MW_ABSTAIN);
    /* An exclusive group: its own hit keeps its prefix, whichever member
     * comes first; what lies behind the group is possible. */
    ASSERT_EQ(mwt_find(&f, "SoftGroup", "trm", NULL, &h), CBM_MW_FOUND);
    ASSERT_TRUE(h.prefix_complete);
    ASSERT_EQ(mwt_find(&f, "SoftFirst", "trm", NULL, &h), CBM_MW_FOUND);
    ASSERT_TRUE(h.prefix_complete);
    ASSERT_EQ(mwt_find(&f, "SoftGroup", "late", NULL, &h), CBM_MW_FOUND);
    ASSERT_FALSE(h.prefix_complete);
    ASSERT_EQ(mwt_find(&f, "SoftFirst", "late", NULL, &h), CBM_MW_FOUND);
    ASSERT_FALSE(h.prefix_complete);
    ASSERT_EQ(mwt_find(&f, "SoftGroup", "nobody", NULL, &h), CBM_MW_ABSTAIN);
    /* An open group: the unseen member may provide it too: possible. */
    ASSERT_EQ(mwt_find(&f, "SoftPlain", "trm", NULL, &h), CBM_MW_FOUND);
    ASSERT_FALSE(h.prefix_complete);
    ASSERT_EQ(mwt_find(&f, "SoftPlain", "nobody", NULL, &h), CBM_MW_ABSTAIN);
    /* Phase 2: an unknown place before the hit: possible; after it: proven. */
    ASSERT_EQ(mwt_find(&f, "SoftLater", "decl", NULL, &h), CBM_MW_FOUND);
    ASSERT_FALSE(h.prefix_complete);
    ASSERT_EQ(mwt_find(&f, "SoftLater", "nobody", NULL, &h), CBM_MW_ABSTAIN);
    ASSERT_EQ(mwt_find(&f, "LaterSoft", "decl", NULL, &h), CBM_MW_FOUND);
    ASSERT_TRUE(h.prefix_complete);
    ASSERT_EQ(mwt_find(&f, "LaterSoft", "nobody", NULL, &h), CBM_MW_ABSTAIN);
    ASSERT_EQ(mwt_find(&f, "HardLater", "decl", NULL, &h), CBM_MW_ABSTAIN);
    /* A type the registry does not hold, reached by NEXT: its own members
     * proven, then a SOFT place: the later interface possible. */
    ASSERT_EQ(mwt_find(&f, "ToGhost", "gm", NULL, &h), CBM_MW_FOUND);
    ASSERT_TRUE(h.prefix_complete);
    ASSERT_EQ(mwt_find(&f, "ToGhost", "decl", NULL, &h), CBM_MW_FOUND);
    ASSERT_FALSE(h.prefix_complete);
    ASSERT_EQ(mwt_find(&f, "ToGhost", "nobody", NULL, &h), CBM_MW_ABSTAIN);
    mwt_close(&f);
    PASS();
}

/* Sealed orders (built once for a shared registry) answer as lazy ones: a
 * receiver registered only through its methods keeps its own members, a name
 * the registry holds in no way is a SOFT place. */
TEST(member_walk_sealed_orders_unheld_types) {
    static const mwt_type_t types[] = {
        {"ToGhost", "O:ToGhost L:Ifc N:Ghost"},
        {"ToVoid", "O:ToVoid L:Ifc N:Void"},
        {"Ifc", "O:Ifc !"},
    };
    mwt_fixture_t f;
    mwt_open(&f, types, 3);
    mwt_add_method(&f.reg, "Ifc", "decl");
    mwt_add_method(&f.reg, "Ghost", "gm");
    cbm_registry_finalize(&f.reg);
    cbm_member_orders_build_all(&f.orders, 1);
    ASSERT_TRUE(f.orders.sealed);
    CBMMemberHit h;
    ASSERT_EQ(mwt_find(&f, "ToGhost", "gm", NULL, &h), CBM_MW_FOUND);
    ASSERT_TRUE(h.prefix_complete);
    ASSERT_EQ(mwt_find(&f, "ToGhost", "decl", NULL, &h), CBM_MW_FOUND);
    ASSERT_FALSE(h.prefix_complete);
    ASSERT_EQ(mwt_find(&f, "ToVoid", "decl", NULL, &h), CBM_MW_FOUND);
    ASSERT_FALSE(h.prefix_complete);
    ASSERT_EQ(mwt_find(&f, "ToVoid", "nobody", NULL, &h), CBM_MW_ABSTAIN);
    ASSERT_EQ(mwt_find(&f, "Ifc", "nobody", NULL, &h), CBM_MW_ABSENT);
    mwt_close(&f);
    PASS();
}

/* A mixin's abstract member is a requirement: the parent's implementation
 * answers (php: a trait's `abstract function m()` with the parent's m runs
 * the parent's). Partners: no implementation anywhere -> the requirement;
 * an implementing sibling trait -> the sibling; an unknown sibling -> unknown. */
TEST(member_walk_mixin_requirement_defers) {
    static const mwt_type_t types[] = {
        {"Kid", "O:Kid X:1 S:Req N:Par"},
        {"Lone", "O:Lone X:1 S:Req !"},
        {"Pair", "O:Pair X:2 S:Req S:Impl !"},
        {"Blind", "O:Blind X:2 S:Req ? N:Par"},
        {"BlindSoft", "O:BlindSoft X:2 S:Req ~ N:Par"},
        {"SoftAfter", "O:SoftAfter X:1 S:Req ~ !"},
        {"SoftParent", "O:SoftParent X:1 S:Req N:SP"},
        {"SoftBetween", "O:SoftBetween X:1 S:Req N:SB"},
        {"Req", "O:Req !"},
        {"Impl", "O:Impl !"},
        {"Par", "O:Par !"},
        {"SP", "O:SP ~ !"},
        {"SB", "O:SB X:1 ~ N:Par"},
    };
    mwt_fixture_t f;
    mwt_open(&f, types, 13);
    CBMRegisteredFunc rf;
    memset(&rf, 0, sizeof(rf));
    rf.min_params = -1;
    rf.receiver_type = "Req";
    rf.short_name = "m";
    rf.qualified_name = "Req.m";
    rf.flags = CBM_FUNC_FLAG_ABSTRACTMETHOD;
    cbm_registry_add_func(&f.reg, rf);
    mwt_add_method(&f.reg, "Impl", "m");
    mwt_add_method(&f.reg, "Par", "m");
    cbm_registry_finalize(&f.reg);
    CBMMemberHit h;
    /* Everything known: the parent implements it (proven); nobody does ->
     * the requirement (proven); a sibling implements it -> the sibling. */
    ASSERT_EQ(mwt_find(&f, "Kid", "m", NULL, &h), CBM_MW_FOUND);
    ASSERT_STR_EQ(h.func->qualified_name, "Par.m");
    ASSERT_EQ(h.place, CBM_MW_PLACE_SUPER);
    ASSERT_TRUE(h.prefix_complete);
    ASSERT_EQ(mwt_find(&f, "Lone", "m", NULL, &h), CBM_MW_FOUND);
    ASSERT_STR_EQ(h.func->qualified_name, "Req.m");
    ASSERT_EQ(h.place, CBM_MW_PLACE_MIXIN);
    ASSERT_TRUE(h.prefix_complete);
    ASSERT_EQ(mwt_find(&f, "Pair", "m", NULL, &h), CBM_MW_FOUND);
    ASSERT_STR_EQ(h.func->qualified_name, "Impl.m");
    ASSERT_EQ(mwt_find(&f, "Blind", "m", NULL, &h), CBM_MW_ABSTAIN);
    /* A SOFT sibling may implement it (php: fx/req2, the vendor trait's m
     * runs): the requirement itself answers, possible; the chain does not go
     * on to the parent. */
    ASSERT_EQ(mwt_find(&f, "BlindSoft", "m", NULL, &h), CBM_MW_FOUND);
    ASSERT_STR_EQ(h.func->qualified_name, "Req.m");
    ASSERT_FALSE(h.prefix_complete);
    /* A SOFT place after the requirement was met (a vendor parent): answered
     * at STOP, possible, never proven (F6). */
    ASSERT_EQ(mwt_find(&f, "SoftAfter", "m", NULL, &h), CBM_MW_FOUND);
    ASSERT_STR_EQ(h.func->qualified_name, "Req.m");
    ASSERT_FALSE(h.prefix_complete);
    ASSERT_EQ(mwt_find(&f, "SoftParent", "m", NULL, &h), CBM_MW_FOUND);
    ASSERT_STR_EQ(h.func->qualified_name, "Req.m");
    ASSERT_FALSE(h.prefix_complete);
    /* A SOFT place between the requirement and an implementation further
     * on: the implementation is not reached; the requirement, possible. */
    ASSERT_EQ(mwt_find(&f, "SoftBetween", "m", NULL, &h), CBM_MW_FOUND);
    ASSERT_STR_EQ(h.func->qualified_name, "Req.m");
    ASSERT_FALSE(h.prefix_complete);
    mwt_close(&f);
    PASS();
}

/* The answer of a sub-order is kept with whether it passed a SOFT place: a
 * second visit through the memo is as soft as the first (F7). Open group
 * [A, B]; A answers H.m through its exclusive group (T beside it passes a
 * SOFT place, which does not mark A's hit); B reaches the same T. The open
 * group's hit is possible because B passed a SOFT place. Partner: without
 * the SOFT place in T the hit is proven. */
TEST(member_walk_memo_keeps_softness) {
    static const mwt_type_t types[] = {
        {"Top", "O:Top G:2 S:A S:B !"},
        {"A", "O:A X:2 S:T S:H !"},
        {"B", "O:B X:1 S:T !"},
        {"T", "O:T X:1 ~ !"},
        {"H", "O:H !"},
        {"Top2", "O:Top2 G:2 S:A2 S:B2 !"},
        {"A2", "O:A2 X:2 S:T2 S:H !"},
        {"B2", "O:B2 X:1 S:T2 !"},
        {"T2", "O:T2 !"},
    };
    mwt_fixture_t f;
    mwt_open(&f, types, 9);
    mwt_add_method(&f.reg, "H", "m");
    cbm_registry_finalize(&f.reg);
    CBMMemberHit h;
    ASSERT_EQ(mwt_find(&f, "A", "m", NULL, &h), CBM_MW_FOUND);
    ASSERT_TRUE(h.prefix_complete); /* exclusive: T's SOFT place does not mark it */
    ASSERT_EQ(mwt_find(&f, "Top", "m", NULL, &h), CBM_MW_FOUND);
    ASSERT_STR_EQ(h.func->qualified_name, "H.m");
    ASSERT_FALSE(h.prefix_complete);
    ASSERT_EQ(mwt_find(&f, "Top", "m", NULL, &h), CBM_MW_FOUND); /* kept answer, same */
    ASSERT_FALSE(h.prefix_complete);
    ASSERT_EQ(mwt_find(&f, "Top2", "m", NULL, &h), CBM_MW_FOUND);
    ASSERT_TRUE(h.prefix_complete);
    mwt_close(&f);
    PASS();
}

/* Interfaces extending each other: a cycle never ends ABSENT (F8). Partner:
 * a diamond (two interfaces with a common parent) is absent. */
TEST(member_walk_later_cycle_abstains) {
    static const mwt_type_t types[] = {
        {"X", "O:X L:I1 !"},
        {"I1", "O:I1 L:I2 !"},
        {"I2", "O:I2 L:I1 !"},
        {"D", "O:D L:J1 L:J2 !"},
        {"J1", "O:J1 L:J0 !"},
        {"J2", "O:J2 L:J0 !"},
        {"J0", "O:J0 !"},
    };
    mwt_fixture_t f;
    mwt_open(&f, types, 7);
    mwt_add_method(&f.reg, "J0", "jm");
    mwt_add_method(&f.reg, "I2", "im");
    cbm_registry_finalize(&f.reg);
    CBMMemberHit h;
    ASSERT_EQ(mwt_find(&f, "X", "nobody", NULL, &h), CBM_MW_ABSTAIN);
    ASSERT_EQ(mwt_find(&f, "X", "im", NULL, &h), CBM_MW_FOUND);
    ASSERT_EQ(mwt_find(&f, "D", "nobody", NULL, &h), CBM_MW_ABSENT);
    ASSERT_EQ(mwt_find(&f, "D", "jm", NULL, &h), CBM_MW_FOUND);
    ASSERT_EQ(h.place, CBM_MW_PLACE_INTERFACE_DEFAULT);
    mwt_close(&f);
    PASS();
}

static void mwl_type(CBMTypeRegistry *reg, const char *qn, bool iface, const CBMSupertype *sup,
                     int nsup, uint32_t flags);

/* `prepare` sees only the types the orders' registry itself holds: a type
 * reached through its fallback (a shared registry) is never handed to it,
 * and the walk leaves it as it was (F8). */
typedef struct {
    int calls;
    int fallback_calls;
    const CBMTypeRegistry *fallback;
} mwp_seen_t;

static void mwp_prepare(void *ctx, CBMRegisteredType *t) {
    mwp_seen_t *s = (mwp_seen_t *)ctx;
    s->calls++;
    const CBMTypeRegistry *fb = s->fallback;
    if (fb && t >= fb->types && t < fb->types + fb->type_count) {
        s->fallback_calls++;
    }
}

TEST(member_walk_prepare_only_own_types) {
    CBMArena arena;
    cbm_arena_init(&arena);
    CBMTypeRegistry shared;
    cbm_registry_init(&shared, &arena);
    static const CBMSupertype base_sup[] = {{"Root", CBM_SUPER_EXTENDS, CBM_SUPER_KNOWN, 0, 0}};
    mwl_type(&shared, "Root", false, NULL, 0, 0);
    mwl_type(&shared, "Base", false, base_sup, 1, 0);
    mwt_add_method(&shared, "Root", "rm");
    cbm_registry_finalize(&shared);
    CBMTypeRegistry head;
    cbm_registry_init(&head, &arena);
    head.fallback = &shared;
    static const CBMSupertype kid_sup[] = {{"Base", CBM_SUPER_EXTENDS, CBM_SUPER_KNOWN, 0, 0}};
    mwl_type(&head, "Kid", false, kid_sup, 1, 0);
    cbm_registry_finalize(&head);
    const CBMRegisteredType *base = cbm_registry_lookup_type(&head, "Base");
    ASSERT_NOT_NULL(base);
    CBMRegisteredType before = *base;
    mwp_seen_t seen = {0, 0, &shared};
    CBMMemberOrders orders;
    cbm_member_orders_init(&orders, &head, cbm_lin_own_traits_parent, &arena);
    orders.prepare = mwp_prepare;
    orders.prepare_ctx = &seen;
    CBMMemberWalker w;
    cbm_member_walker_init(&w, &orders);
    CBMMemberQuery q;
    memset(&q, 0, sizeof(q));
    q.reg = &head;
    q.type_qn = "Kid";
    q.name = "rm";
    CBMMemberHit h;
    ASSERT_EQ(cbm_member_lookup(&w, &q, &h), CBM_MW_FOUND);
    ASSERT_STR_EQ(h.func->qualified_name, "Root.rm");
    ASSERT_EQ(seen.calls, 1); /* Kid only */
    ASSERT_EQ(seen.fallback_calls, 0);
    ASSERT_EQ(memcmp(&before, base, sizeof(before)), 0);
    cbm_member_walker_free(&w);
    cbm_member_orders_free(&orders);
    cbm_arena_destroy(&arena);
    PASS();
}

/* ── the PHP lineariser, from supertypes ───────────────────────────── */

typedef struct {
    CBMArena arena;
    CBMTypeRegistry reg;
    CBMMemberOrders orders;
    CBMMemberWalker walker;
} mwl_fixture_t;

static void mwl_type(CBMTypeRegistry *reg, const char *qn, bool iface, const CBMSupertype *sup,
                     int nsup, uint32_t flags) {
    CBMRegisteredType rt;
    memset(&rt, 0, sizeof(rt));
    rt.qualified_name = qn;
    rt.short_name = qn;
    rt.is_interface = iface;
    rt.supertypes = sup;
    rt.supertype_count = nsup;
    rt.member_flags = flags;
    cbm_registry_add_type(reg, rt);
}

static CBMMemberWalkResult mwl_find(mwl_fixture_t *f, const char *type, const char *name,
                                   CBMMemberHit *h) {
    CBMMemberQuery q;
    memset(&q, 0, sizeof(q));
    q.reg = &f->reg;
    q.type_qn = type;
    q.name = name;
    return cbm_member_lookup(&f->walker, &q, h);
}

TEST(member_walk_lin_own_traits_parent) {
    mwl_fixture_t f;
    cbm_arena_init(&f.arena);
    cbm_registry_init(&f.reg, &f.arena);
    static const CBMSupertype kid_sup[] = {
        {"Base", CBM_SUPER_EXTENDS, CBM_SUPER_KNOWN, 0, 0},
        {"Ifc", CBM_SUPER_IMPLEMENTS, CBM_SUPER_KNOWN, 0, 0},
        {"T", CBM_SUPER_TRAIT, CBM_SUPER_KNOWN, 0, 0},
    };
    static const CBMSupertype vend_sup[] = {
        {"Vendor\\Model", CBM_SUPER_EXTENDS, CBM_SUPER_EXTERNAL, 0, 0},
        {"Ifc", CBM_SUPER_IMPLEMENTS, CBM_SUPER_KNOWN, 0, 0},
    };
    static const CBMSupertype ifc_sup[] = {
        {"IfcParent", CBM_SUPER_EXTENDS, CBM_SUPER_KNOWN, 0, 0},
    };
    static const CBMSupertype lost_trait_sup[] = {
        {"T", CBM_SUPER_TRAIT, CBM_SUPER_KNOWN, 0, 0},
        {"Gone", CBM_SUPER_TRAIT, CBM_SUPER_UNRESOLVED, 0, 0},
        {"Base", CBM_SUPER_EXTENDS, CBM_SUPER_KNOWN, 0, 0},
    };
    static const CBMSupertype two_parents[] = {
        {"Base", CBM_SUPER_EXTENDS, CBM_SUPER_KNOWN, 0, 0},
        {"Other", CBM_SUPER_EXTENDS, CBM_SUPER_KNOWN, 0, 0},
    };
    static const CBMSupertype vend_ifc_first[] = {
        {"Vendor\\Contract", CBM_SUPER_IMPLEMENTS, CBM_SUPER_EXTERNAL, 0, 0},
        {"Ifc", CBM_SUPER_IMPLEMENTS, CBM_SUPER_KNOWN, 0, 0},
    };
    static const CBMSupertype vend_ifc_last[] = {
        {"Ifc", CBM_SUPER_IMPLEMENTS, CBM_SUPER_KNOWN, 0, 0},
        {"Vendor\\Contract", CBM_SUPER_IMPLEMENTS, CBM_SUPER_EXTERNAL, 0, 0},
    };
    static const CBMSupertype ghost_parent[] = {
        {"Ghost", CBM_SUPER_EXTENDS, CBM_SUPER_KNOWN, 0, 0}, /* known name, no registered type */
        {"Ifc", CBM_SUPER_IMPLEMENTS, CBM_SUPER_KNOWN, 0, 0},
    };
    mwl_type(&f.reg, "Kid", false, kid_sup, 3, 0);
    mwl_type(&f.reg, "Base", false, NULL, 0, 0);
    mwl_type(&f.reg, "Other", false, NULL, 0, 0);
    mwl_type(&f.reg, "T", false, NULL, 0, 0);
    mwl_type(&f.reg, "Ifc", true, ifc_sup, 1, 0);
    mwl_type(&f.reg, "IfcParent", true, NULL, 0, 0);
    mwl_type(&f.reg, "OnVendor", false, vend_sup, 2, 0);
    mwl_type(&f.reg, "LostTrait", false, lost_trait_sup, 3, 0);
    mwl_type(&f.reg, "Magic", false, kid_sup, 1, CBM_MEMBERS_OPEN);
    mwl_type(&f.reg, "TwoParents", false, two_parents, 2, 0);
    mwl_type(&f.reg, "Torn", false, kid_sup, 1, CBM_MEMBERS_DECLS_DISAGREE);
    mwl_type(&f.reg, "VendIfcFirst", false, vend_ifc_first, 2, 0);
    mwl_type(&f.reg, "VendIfcLast", false, vend_ifc_last, 2, 0);
    mwl_type(&f.reg, "OnGhost", false, ghost_parent, 2, 0);
    mwt_add_method(&f.reg, "Kid", "own");
    mwt_add_method(&f.reg, "OnVendor", "mine");
    mwt_add_method(&f.reg, "Ghost", "gm"); /* a receiver only: Ghost is no registered type */
    mwt_add_method(&f.reg, "Base", "run");
    mwt_add_method(&f.reg, "T", "run"); /* the trait beats the parent */
    mwt_add_method(&f.reg, "T", "tonly");
    mwt_add_method(&f.reg, "Base", "bonly");
    mwt_add_method(&f.reg, "Ifc", "bonly"); /* the parent beats the interface */
    mwt_add_method(&f.reg, "IfcParent", "deep");
    mwt_add_method(&f.reg, "Ifc", "idecl");
    cbm_registry_finalize(&f.reg);
    cbm_member_orders_init(&f.orders, &f.reg, cbm_lin_own_traits_parent, &f.arena);
    cbm_member_walker_init(&f.walker, &f.orders);
    CBMMemberHit h;
    /* A completely known chain: every hit proven. */
    ASSERT_EQ(mwl_find(&f, "Kid", "run", &h), CBM_MW_FOUND);
    ASSERT_STR_EQ(h.func->qualified_name, "T.run");
    ASSERT_EQ(h.place, CBM_MW_PLACE_MIXIN);
    ASSERT_STR_EQ(h.via_qn, "Kid");
    ASSERT_TRUE(h.prefix_complete);
    ASSERT_EQ(mwl_find(&f, "Kid", "bonly", &h), CBM_MW_FOUND);
    ASSERT_STR_EQ(h.func->qualified_name, "Base.bonly");
    ASSERT_EQ(h.place, CBM_MW_PLACE_SUPER);
    ASSERT_TRUE(h.prefix_complete);
    ASSERT_EQ(mwl_find(&f, "Kid", "deep", &h), CBM_MW_FOUND);
    ASSERT_EQ(h.place, CBM_MW_PLACE_INTERFACE_DEFAULT);
    ASSERT_TRUE(h.prefix_complete);
    ASSERT_EQ(mwl_find(&f, "Kid", "nobody", &h), CBM_MW_ABSENT);
    /* A parent outside the project: passed; an interface's declaration
     * behind it is only a possible answer, and nothing is absent. Partner:
     * the class's own member, before the unknown place, is proven. */
    ASSERT_EQ(mwl_find(&f, "OnVendor", "idecl", &h), CBM_MW_FOUND);
    ASSERT_FALSE(h.prefix_complete);
    ASSERT_EQ(mwl_find(&f, "OnVendor", "nobody", &h), CBM_MW_ABSTAIN);
    ASSERT_EQ(mwl_find(&f, "OnVendor", "mine", &h), CBM_MW_FOUND);
    ASSERT_TRUE(h.prefix_complete);
    /* An interface outside the project before the one that declares the
     * member: possible; after it: proven. Nothing absent in either. */
    ASSERT_EQ(mwl_find(&f, "VendIfcFirst", "idecl", &h), CBM_MW_FOUND);
    ASSERT_FALSE(h.prefix_complete);
    ASSERT_EQ(mwl_find(&f, "VendIfcFirst", "nobody", &h), CBM_MW_ABSTAIN);
    ASSERT_EQ(mwl_find(&f, "VendIfcLast", "idecl", &h), CBM_MW_FOUND);
    ASSERT_TRUE(h.prefix_complete);
    ASSERT_EQ(mwl_find(&f, "VendIfcLast", "nobody", &h), CBM_MW_ABSTAIN);
    /* A known parent name the registry does not hold as a type: the walk's
     * own order for it, [OWN, SOFT UNSEEN, STOP]: its methods proven, the
     * interface behind it possible, nothing absent. */
    ASSERT_EQ(mwl_find(&f, "OnGhost", "gm", &h), CBM_MW_FOUND);
    ASSERT_STR_EQ(h.func->qualified_name, "Ghost.gm");
    ASSERT_TRUE(h.prefix_complete);
    ASSERT_EQ(mwl_find(&f, "OnGhost", "idecl", &h), CBM_MW_FOUND);
    ASSERT_FALSE(h.prefix_complete);
    ASSERT_EQ(mwl_find(&f, "OnGhost", "nobody", &h), CBM_MW_ABSTAIN);
    /* An unresolved trait beside a known one: the known trait's member
     * answers, proven (valid programs have one provider); the parent's
     * behind it only as possible. */
    ASSERT_EQ(mwl_find(&f, "LostTrait", "tonly", &h), CBM_MW_FOUND);
    ASSERT_TRUE(h.prefix_complete);
    ASSERT_EQ(mwl_find(&f, "LostTrait", "bonly", &h), CBM_MW_FOUND);
    ASSERT_STR_EQ(h.func->qualified_name, "Base.bonly");
    ASSERT_FALSE(h.prefix_complete);
    ASSERT_EQ(mwl_find(&f, "LostTrait", "nobody", &h), CBM_MW_ABSTAIN);
    /* __call: nothing declared is unknown; declared members answer. */
    ASSERT_EQ(mwl_find(&f, "Magic", "nobody", &h), CBM_MW_ABSTAIN);
    ASSERT_EQ(mwl_find(&f, "Magic", "bonly", &h), CBM_MW_FOUND);
    /* Two parents (sources disagree): the chain past the own members is
     * unknown. */
    ASSERT_EQ(mwl_find(&f, "TwoParents", "run", &h), CBM_MW_ABSTAIN);
    ASSERT_EQ(mwl_find(&f, "Torn", "own", &h), CBM_MW_ABSTAIN);
    /* An interface looked up directly: its own, then its parents. */
    ASSERT_EQ(mwl_find(&f, "Ifc", "deep", &h), CBM_MW_FOUND);
    ASSERT_EQ(mwl_find(&f, "Ifc", "nobody", &h), CBM_MW_ABSENT);
    /* A type nobody registered: own members (proven), then a SOFT place:
     * nothing absent. */
    mwt_add_method(&f.reg, "Loose", "lm");
    ASSERT_EQ(mwl_find(&f, "Loose", "lm", &h), CBM_MW_FOUND);
    ASSERT_TRUE(h.prefix_complete);
    ASSERT_EQ(mwl_find(&f, "Loose", "nobody", &h), CBM_MW_ABSTAIN);
    cbm_member_walker_free(&f.walker);
    cbm_member_orders_free(&f.orders);
    cbm_arena_destroy(&f.arena);
    PASS();
}

/* ── depth: no C stack per level (F1) ─────────────────────────────── */

typedef enum { MWD_TRAITS, MWD_PARENTS, MWD_INTERFACES } mwd_shape_t;

/* What a deep lookup found, as plain values (the hit's strings die with
 * the fixture's arena). */
typedef struct {
    uint8_t place;
    uint8_t mixin_depth;
    bool via_is_top;
} mwd_found_t;

/* A 40,000-deep chain of trait uses, of parents or of interfaces, through
 * the PHP lineariser: the member at the bottom is found from the top type,
 * and a name nobody declares is absent (everything is known). */
static int mwd_depth(mwd_shape_t shape, int n, mwd_found_t *found) {
    CBMMemberHit hitv;
    CBMMemberHit *hit = &hitv;
    CBMArena arena;
    cbm_arena_init(&arena);
    CBMTypeRegistry reg;
    cbm_registry_init(&reg, &arena);
    CBMSupertype *sup = (CBMSupertype *)cbm_arena_alloc(&arena, (size_t)(n + 2) * sizeof(*sup));
    const char **names = (const char **)cbm_arena_alloc(&arena, (size_t)(n + 2) * sizeof(*names));
    for (int i = 0; i <= n + 1; i++) {
        names[i] = cbm_arena_sprintf(&arena, "Ty%d", i);
    }
    uint8_t kind = shape == MWD_TRAITS    ? CBM_SUPER_TRAIT
                   : shape == MWD_PARENTS ? CBM_SUPER_EXTENDS
                                          : CBM_SUPER_IMPLEMENTS;
    for (int i = 0; i <= n; i++) {
        if (i > 0) {
            sup[i].type_qn = names[i - 1];
            sup[i].kind = shape == MWD_INTERFACES ? CBM_SUPER_EXTENDS : kind;
            sup[i].state = CBM_SUPER_KNOWN;
        }
        mwl_type(&reg, names[i], shape == MWD_INTERFACES, i > 0 ? &sup[i] : NULL, i > 0 ? 1 : 0,
                 0);
    }
    /* The top: a class using / extending / implementing the deepest one. */
    sup[n + 1].type_qn = names[n];
    sup[n + 1].kind = kind;
    sup[n + 1].state = CBM_SUPER_KNOWN;
    mwl_type(&reg, names[n + 1], false, &sup[n + 1], 1, 0);
    mwt_add_method(&reg, names[0], "bottom");
    cbm_registry_finalize(&reg);
    CBMMemberOrders orders;
    cbm_member_orders_init(&orders, &reg, cbm_lin_own_traits_parent, &arena);
    CBMMemberWalker w;
    cbm_member_walker_init(&w, &orders);
    CBMMemberQuery q;
    memset(&q, 0, sizeof(q));
    q.reg = &reg;
    q.type_qn = names[n + 1];
    q.name = "bottom";
    int ok = cbm_member_lookup(&w, &q, hit) == CBM_MW_FOUND && hit->func &&
             strcmp(hit->func->qualified_name, "Ty0.bottom") == 0 && hit->prefix_complete;
    found->place = hit->place;
    found->mixin_depth = hit->mixin_depth;
    found->via_is_top = hit->via_qn && strcmp(hit->via_qn, names[n + 1]) == 0;
    CBMMemberHit miss;
    q.name = "nobody";
    ok = ok && cbm_member_lookup(&w, &q, &miss) == CBM_MW_ABSENT;
    cbm_member_walker_free(&w);
    cbm_member_orders_free(&orders);
    cbm_arena_destroy(&arena);
    return ok;
}

TEST(member_walk_depth_needs_no_stack) {
    mwd_found_t h;
    ASSERT_TRUE(mwd_depth(MWD_TRAITS, 40000, &h));
    ASSERT_EQ(h.place, CBM_MW_PLACE_MIXIN);
    ASSERT_TRUE(h.via_is_top);
    ASSERT_EQ(h.mixin_depth, 255); /* capped */
    ASSERT_TRUE(mwd_depth(MWD_PARENTS, 40000, &h));
    ASSERT_EQ(h.place, CBM_MW_PLACE_SUPER);
    ASSERT_EQ(h.mixin_depth, 0);
    ASSERT_TRUE(mwd_depth(MWD_INTERFACES, 40000, &h));
    ASSERT_EQ(h.place, CBM_MW_PLACE_INTERFACE_DEFAULT);
    /* Partner: one level, the same answers. */
    ASSERT_TRUE(mwd_depth(MWD_TRAITS, 1, &h));
    ASSERT_EQ(h.mixin_depth, 2);
    PASS();
}

/* ── growth, counted at 1x, 4x, 16x ────────────────────────────────── */

typedef enum { MWG_CHAIN, MWG_MANY_TRAITS, MWG_SHARED_TRAIT } mwg_shape_t;

/* The total work (steps read, orders built, index entries made) of all the
 * lookups a shape of size n asks, through the PHP lineariser: every lookup
 * of a file shares one walker, as in the PHP engine. */
static uint64_t mwg_work(mwg_shape_t shape, int n) {
    CBMArena arena;
    cbm_arena_init(&arena);
    CBMTypeRegistry reg;
    cbm_registry_init(&reg, &arena);
    CBMSupertype *sup = (CBMSupertype *)cbm_arena_alloc(&arena, (size_t)(n + 1) * sizeof(*sup));
    const char **names = (const char **)cbm_arena_alloc(&arena, (size_t)(n + 1) * sizeof(*names));
    for (int i = 0; i <= n; i++) {
        names[i] = cbm_arena_sprintf(&arena, "Ty%d", i);
    }
    if (shape == MWG_CHAIN) {
        /* Ty0 <- Ty1 <- ... <- Tyn; a method on the root, looked up from
         * every class, and a miss from every class. */
        for (int i = 0; i <= n; i++) {
            if (i > 0) {
                sup[i].type_qn = names[i - 1];
                sup[i].kind = CBM_SUPER_EXTENDS;
                sup[i].state = CBM_SUPER_KNOWN;
            }
            mwl_type(&reg, names[i], false, i > 0 ? &sup[i] : NULL, i > 0 ? 1 : 0, 0);
        }
        mwt_add_method(&reg, names[0], "root");
    } else if (shape == MWG_MANY_TRAITS) {
        /* One class using n traits, each with its own method; every one of
         * the n names looked up on the class, and a miss. */
        for (int i = 1; i <= n; i++) {
            sup[i - 1].type_qn = names[i];
            sup[i - 1].kind = CBM_SUPER_TRAIT;
            sup[i - 1].state = CBM_SUPER_KNOWN;
            mwl_type(&reg, names[i], false, NULL, 0, 0);
            mwt_add_method(&reg, names[i], cbm_arena_sprintf(&arena, "m%d", i));
        }
        mwl_type(&reg, names[0], false, sup, n, 0);
    } else {
        /* n classes all using one trait Ty0; its method looked up from each. */
        sup[0].type_qn = names[0];
        sup[0].kind = CBM_SUPER_TRAIT;
        sup[0].state = CBM_SUPER_KNOWN;
        mwl_type(&reg, names[0], false, NULL, 0, 0);
        mwt_add_method(&reg, names[0], "shared");
        for (int i = 1; i <= n; i++) {
            mwl_type(&reg, names[i], false, sup, 1, 0);
        }
    }
    cbm_registry_finalize(&reg);
    CBMMemberOrders orders;
    cbm_member_orders_init(&orders, &reg, cbm_lin_own_traits_parent, &arena);
    CBMMemberWalker w;
    cbm_member_walker_init(&w, &orders);
    CBMMemberQuery q;
    memset(&q, 0, sizeof(q));
    q.reg = &reg;
    CBMMemberHit h;
    uint64_t found = 0;
    uint64_t want = 0;
    if (shape == MWG_CHAIN) {
        /* From every class, the deepest first: n + 1 lookups of the root's
         * method over a chain of n + 1 classes, and n + 1 misses. */
        for (int i = n; i >= 0; i--) {
            q.type_qn = names[i];
            q.name = "root";
            found += cbm_member_lookup(&w, &q, &h) == CBM_MW_FOUND;
            q.name = "nobody";
            found += cbm_member_lookup(&w, &q, &h) == CBM_MW_ABSENT;
        }
        want = 2 * (uint64_t)(n + 1);
    } else if (shape == MWG_MANY_TRAITS) {
        q.type_qn = names[0];
        for (int i = 1; i <= n; i++) {
            q.name = cbm_arena_sprintf(&arena, "m%d", i);
            found += cbm_member_lookup(&w, &q, &h) == CBM_MW_FOUND;
        }
        q.name = "nobody";
        found += cbm_member_lookup(&w, &q, &h) == CBM_MW_ABSENT;
        want = (uint64_t)n + 1;
    } else {
        for (int i = 1; i <= n; i++) {
            q.type_qn = names[i];
            q.name = "shared";
            found += cbm_member_lookup(&w, &q, &h) == CBM_MW_FOUND;
        }
        want = (uint64_t)n;
    }
    uint64_t work = w.work + orders.steps;
    cbm_member_walker_free(&w);
    cbm_member_orders_free(&orders);
    cbm_arena_destroy(&arena);
    return found == want ? work : 0;
}

TEST(member_walk_growth_is_linear) {
    static const char *shape_names[] = {"deep chain", "class using n traits",
                                        "trait used by n classes"};
    for (int s = 0; s < 3; s++) {
        uint64_t w1 = mwg_work((mwg_shape_t)s, 250);
        uint64_t w4 = mwg_work((mwg_shape_t)s, 1000);
        uint64_t w16 = mwg_work((mwg_shape_t)s, 4000);
        printf("  growth %-24s n=250 %llu, n=1000 %llu, n=4000 %llu (x%.2f, x%.2f)\n",
               shape_names[s], (unsigned long long)w1, (unsigned long long)w4,
               (unsigned long long)w16, w1 ? (double)w4 / (double)w1 : 0.0,
               w4 ? (double)w16 / (double)w4 : 0.0);
        ASSERT(w1 > 0);
        ASSERT(w4 > 0);
        ASSERT(w16 > 0);
        /* Linear: each x4 in size costs at most x4.5 in work. */
        ASSERT(w4 * 2 <= w1 * 9);
        ASSERT(w16 * 2 <= w4 * 9);
    }
    PASS();
}

SUITE(member_walk) {
    RUN_TEST(member_walk_group_answers_once_or_abstains);
    RUN_TEST(member_walk_searches_the_given_order);
    RUN_TEST(member_walk_end_without_stop_is_unknown);
    RUN_TEST(member_walk_ambiguous_own_member_abstains);
    RUN_TEST(member_walk_work_is_one_per_step);
    RUN_TEST(member_walk_exclusive_group_with_unknown_member);
    RUN_TEST(member_walk_hit_reports_place_and_completeness);
    RUN_TEST(member_walk_open_turns_absent_into_unknown);
    RUN_TEST(member_walk_later_needs_a_complete_chain);
    RUN_TEST(member_walk_renames_alias_and_exclude);
    RUN_TEST(member_walk_cycles_abstain_diamonds_agree);
    RUN_TEST(member_walk_fields);
    RUN_TEST(member_walk_collect_modes);
    RUN_TEST(member_walk_soft_unknown_places);
    RUN_TEST(member_walk_sealed_orders_unheld_types);
    RUN_TEST(member_walk_mixin_requirement_defers);
    RUN_TEST(member_walk_memo_keeps_softness);
    RUN_TEST(member_walk_later_cycle_abstains);
    RUN_TEST(member_walk_prepare_only_own_types);
    RUN_TEST(member_walk_lin_own_traits_parent);
    RUN_TEST(member_walk_depth_needs_no_stack);
    RUN_TEST(member_walk_growth_is_linear);
}
