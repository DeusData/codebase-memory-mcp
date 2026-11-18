/*
 * member_walk.h — one member lookup over a type's ancestry, shared by every
 * object-oriented engine.
 *
 * Lifted from the parked Ruby branch (feat/ruby-cross-file-lookup, commit
 * 96547561c) and corrected for the other engines. Every engine answers "type
 * T, member m" by searching the members of a sequence of types in an order
 * the language defines: PHP's own members, then its traits, then the parent;
 * Python's C3 linearisation; Go's embedding depths. The ORDER is the
 * language's (a lineariser turns CBMRegisteredType.supertypes into steps);
 * the SEARCH over it is this file's, the same for every language.
 *
 * Three answers, and only three:
 *
 *   CBM_MW_FOUND    a step declares the member: the hit says which, where
 *                   (own, superclass, mixin, interface declaration, ...) and
 *                   whether every step before it was completely known;
 *   CBM_MW_ABSENT   the walk reached an explicit STOP step: the ancestry is
 *                   completely known and declares no such member. A
 *                   lineariser writes STOP only after a fully known ancestry,
 *                   and no SOFT place was passed on the way;
 *   CBM_MW_ABSTAIN  anything else: a hard UNSEEN step, a type that cannot be
 *                   described, two definitions where one is needed, a cycle,
 *                   the END of the steps, or a STOP reached after a SOFT place.
 *                   A builder that forgets a STOP or an UNSEEN gets "unknown",
 *                   never "not found" (the Ruby branch's failure: "unknown"
 *                   read as "not found" removed right edges). ABSTAIN is the
 *                   zero value for that reason.
 *
 * Possible against proven (CBMMemberHit.prefix_complete). A FOUND hit with a
 * complete prefix is PROVEN: every step before it was completely known. A hit
 * with an incomplete prefix is POSSIBLE: the walk passed a SOFT place (a
 * supertype the registry does not hold, or one not resolved), or an OUTSIDE
 * step whose position nobody knows, and found the member behind it; what it
 * passed may declare the member first. An engine decides what it does with a
 * possible answer; the walk never decides. Behind a SOFT place the walk never
 * answers ABSENT.
 *
 * Steps (an order is an array of them, first searched first):
 *
 *   OWN     the members (type_id, side) itself declares;
 *   UNSEEN  something nobody can see may answer here. A hard one (the
 *           default: the type's own members are not known, e.g. computed)
 *           ends the walk: unknown. A SOFT one (CBM_MW_SOFT) stands for a type
 *           the registry does not hold (outside the project, not resolved):
 *           it is passed; what the walk finds behind it is FOUND with
 *           prefix_complete false (possible), and the walk can no longer end
 *           ABSENT. The walk's own order for a type the registry does not hold
 *           is [OWN, SOFT UNSEEN, STOP] (its own members, if any, are proven);
 *   STOP    nothing beyond this point: absent (phase 1 ends, see LATER);
 *   NEXT    the last step only: the order goes on as (type_id, side)'s order.
 *           Single inheritance links to the parent's order instead of copying
 *           it, so a chain of n types costs n steps in total;
 *   GROUP   the next `count` steps (OWN, SUB or UNSEEN each) are one place whose
 *           members' relative order nobody knows. With CBM_MW_GROUP_EXCLUSIVE
 *           (PHP traits, Go embedding at one depth: a valid program has at most
 *           one provider of a name) one distinct definition answers even when
 *           another member is unknown, and a SOFT member leaves its prefix as
 *           it was; without it every member must be known, and a hit beside a
 *           SOFT member is only possible. Members are siblings: each is
 *           walked from the state before the group. Two distinct definitions
 *           abstain. None goes on after the group;
 *   SUB     the first answer of (type_id, side)'s whole order (a trait using
 *           traits, a mixin with its own ancestry);
 *   LATER   (type_id, side)'s order is searched only after the chain ended at
 *           a STOP without a hit (interface declarations, abstract members):
 *           phase 2, breadth first, each type once, first hit wins; a cycle
 *           among the entries searched answers ABSTAIN, never ABSENT. A LATER
 *           step with type_id NULL is an unknown place in phase 2 (hard or
 *           SOFT, as UNSEEN);
 *   OPEN    the type dispatches undeclared names (CBM_MEMBERS_OPEN): a STOP
 *           reached after it answers ABSTAIN instead of ABSENT.
 *
 * A mixin's abstract member (CBM_FUNC_FLAG_ABSTRACTMETHOD at a MIXIN place:
 * a PHP trait's `abstract function m()`) is a requirement, not an
 * implementation. It defers to later places only while every place that
 * could satisfy it before them is known: met with nothing unknown beside it
 * (same group) or before it, the walk goes on, and an implementation found
 * further on answers (proven); the requirement answers itself at the chain's
 * STOP (proven only when nothing SOFT lies anywhere on the way). Met beside
 * or after a SOFT place, or when a SOFT place lies between it and the next
 * implementation, the requirement itself is the answer, possible, and the
 * chain does not go on. In a group it never conflicts with an implementation;
 * beside a hard unknown member it is unknown.
 *
 * Depth and memory. Nothing in the walk uses C stack depth that grows with
 * the input: a mixin's whole order and the rest of a chain past a NEXT link
 * are nodes answered on an explicit stack of frames (heap), each answered
 * once per walker and kept (with whether it passed a SOFT place), so n
 * lookups over one ancestry cost the ancestry once. A group of at least
 * eight members finds the members that declare a name through an index of
 * its leaf members (a member that is its type's own members only); renames
 * are found by name. Every step read, order built and index entry counts one
 * unit of work (CBMMemberWalker.work, CBM_LSP_WORK). Nothing caps an order's
 * length or depth. Cycles (a class extending itself through others, a trait
 * using itself, interfaces extending each other) abstain.
 *
 * Threading. A walker serves one thread and keeps its answers for one
 * registry, member kind and overload mode (another one clears them); the
 * registry must not change while the walker lives (PHP: per file). Orders
 * built lazily belong to that thread too. Sealed orders
 * (cbm_member_orders_build_all) are read-only and may be shared by walkers on
 * many threads. `prepare` is only ever called with a type the orders'
 * registry itself holds, never one found through its fallback: a shared
 * fallback registry's types are settled before it is sealed and are only read.
 *
 * Known limits for the next engines (no code yet; the slice that needs each):
 *   - Phase 2 is breadth first and its first hit wins. Java's and C#'s
 *     "most specific default method" (an interface overriding its parent's
 *     default; two unrelated defaults conflict) is not expressible: the Java /
 *     C# engine slices.
 *   - A SUB is its type's whole order, phase 2 included: a mixin with LATER
 *     steps answers its interface declarations before the outer chain's
 *     parents are searched. The Ruby and Scala (trait with interfaces) slices.
 *   - Go embedding needs groups of OWN members per depth (a SUB member's
 *     deeper hit conflicts with a shallower sibling's and the group abstains:
 *     recall lost, no false edge) unless the lineariser flattens the depths:
 *     the Go slice.
 *   - cbm_member_orders_build_all covers the head registry's types and
 *     receivers only, not a fallback's: the first engine with chained shared
 *     registries (TypeScript, Python Tier 2).
 *   - The sealed path and CBMTypeRegistry.member_orders have no production
 *     user yet; their thread safety is tested in unit tests only: the first
 *     engine that shares orders across threads.
 *   - Arity and argument-type selection of overloads is the engine's, after
 *     COLLECT: the Java / Kotlin / C# slices add selectors with their reader.
 *   - Sequential SUB steps that are not grouped (Ruby includes) cost one pass
 *     of the order per SUB step not yet answered (quadratic in the number of
 *     such steps of one order, once per name): the Ruby slice.
 *   - Phase 2 is answered once per (start type, name); n lookups from n
 *     different classes of one n-deep chain whose classes each implement an
 *     interface, for a name only the interfaces declare, search the
 *     interfaces of every level each time (quadratic in that shape): the
 *     first engine whose phase 2 holds more than interface declarations.
 */
#ifndef CBM_LSP_MEMBER_WALK_H
#define CBM_LSP_MEMBER_WALK_H

#include "type_registry.h"
#include <stdbool.h>
#include <stdint.h>

/* The sides a language may look members up on (Ruby: instance and class). */
enum { CBM_MW_SIDES = 2 };

typedef enum {
    CBM_MW_OWN = 0,
    CBM_MW_UNSEEN,
    CBM_MW_STOP,
    CBM_MW_NEXT,
    CBM_MW_GROUP,
    CBM_MW_SUB,
    CBM_MW_LATER,
    CBM_MW_OPEN,
} CBMMemberStepKind;

/* Where a hit lies, seen from the type the lookup started on. */
typedef enum {
    CBM_MW_PLACE_OWN = 0,           /* the start type's own members */
    CBM_MW_PLACE_SUPER,             /* a superclass's own members */
    CBM_MW_PLACE_INTERFACE_DEFAULT, /* an interface's member, reached in phase 2 */
    CBM_MW_PLACE_MIXIN,             /* a trait / module / embedded type */
    CBM_MW_PLACE_PREPEND_IN_BODY,   /* Ruby: a module prepended in the class body */
    CBM_MW_PLACE_OUTSIDE,           /* a mixin applied outside the type's body */
} CBMMemberPlace;

enum {
    CBM_MW_GROUP_EXCLUSIVE = 1u << 0, /* GROUP: valid programs have at most one provider */
    CBM_MW_SOFT = 1u << 1,            /* UNSEEN, LATER NULL, a group's UNSEEN member: a type the
                                         registry does not hold, passed (behind it: possible) */
};

typedef struct {
    const char *type_id; /* OWN, SUB, LATER, NEXT: the type */
    const char *via;     /* GROUP: the type whose order holds the group (renames, place) */
    int count;           /* GROUP: the number of member steps that follow */
    int side;            /* OWN, SUB, LATER, NEXT */
    uint8_t kind;        /* CBMMemberStepKind */
    uint8_t place;       /* CBMMemberPlace of a hit at an OWN or SUB step */
    uint8_t flags;       /* GROUP: CBM_MW_GROUP_* */
} CBMMemberStep;

typedef enum {
    CBM_MW_ABSTAIN = 0, /* unknown: no answer, the caller's other rules stay */
    CBM_MW_FOUND,
    CBM_MW_ABSENT, /* only at an explicit STOP */
} CBMMemberWalkResult;

typedef enum {
    CBM_MW_MEMBER_METHOD = 1,
    CBM_MW_MEMBER_FIELD = 2,
    CBM_MW_MEMBER_ANY = 3, /* a method, else a field */
} CBMMemberKind;

typedef enum {
    CBM_MW_HIDE_BY_NAME = 0,  /* the first place declaring the name answers (PHP, Python, Ruby) */
    CBM_MW_COLLECT,           /* every declaration of the name in the order (Java, Kotlin) */
    CBM_MW_HIDE_BY_SIGNATURE, /* every declaration, a derived one hiding an equal signature (C#) */
} CBMOverloadMode;

/* A lineariser: the steps of (t, side)'s order, appended to `out`. Returns
 * false when the type cannot be described (the lookup then abstains). The
 * steps' strings must outlive the orders (registry or arena strings). */
struct CBMMemberOrders;
struct CBMMemberSteps;
typedef bool (*CBMLineariser)(struct CBMMemberOrders *o, const CBMRegisteredType *t, int side,
                              struct CBMMemberSteps *out);

/* Optional, per-file registries only: settle a type's supertypes (resolve
 * spellings, set states and member flags) the first time its order is
 * built. Never called on sealed orders, and only with a type the orders'
 * registry itself holds (never one found through its fallback). */
typedef void (*CBMMemberPrepare)(void *ctx, CBMRegisteredType *t);

typedef struct CBMMemberSteps {
    CBMMemberStep *items;
    int count;
    int cap;
    bool oom; /* a push could not grow the list: the order is incomplete */
} CBMMemberSteps;

void cbm_member_steps_push(CBMMemberSteps *s, CBMMemberStepKind kind, const char *type_id, int side,
                           CBMMemberPlace place);
/* A GROUP header; the caller pushes `count` member steps right after it. */
void cbm_member_steps_group(CBMMemberSteps *s, const char *via, int count, uint8_t flags);
/* A SOFT unknown place: an UNSEEN step, or (kind LATER) an unknown place in
 * phase 2. */
void cbm_member_steps_soft(CBMMemberSteps *s, CBMMemberStepKind kind, int side,
                           CBMMemberPlace place);
void cbm_member_steps_free(CBMMemberSteps *s);

/* The orders of one registry, built once per (type, side) and cached. */
typedef struct CBMMemberOrders {
    const CBMTypeRegistry *reg;
    CBMLineariser lin;
    CBMMemberPrepare prepare; /* may be NULL */
    void *prepare_ctx;
    CBMArena *arena; /* the cached steps live here */
    void *by_type;   /* CBMHashTable: type QN -> cached orders of its sides */
    bool sealed;     /* shared read-only: a missing order is never built */
    uint64_t built;  /* orders built (counted work) */
    uint64_t steps;  /* steps stored in all orders (memory accounting) */
    CBMMemberSteps scratch;
} CBMMemberOrders;

/* Orders over `reg` built by `lin`, cached in `arena`. */
void cbm_member_orders_init(CBMMemberOrders *o, const CBMTypeRegistry *reg, CBMLineariser lin,
                            CBMArena *arena);
void cbm_member_orders_free(CBMMemberOrders *o);
/* Build every registered type's orders, and every method receiver's, for
 * `sides` sides, then refuse lazy builds (a shared registry: build before the
 * registry's seal). */
void cbm_member_orders_build_all(CBMMemberOrders *o, int sides);
/* The order of (type_qn, side), built on first use unless sealed. A type the
 * registry does not hold gets [OWN type, SOFT UNSEEN, STOP]: its own members
 * can still answer (proven), anything behind them is only possible; sealed,
 * a name built neither as a type nor as a receiver gets [SOFT UNSEEN, STOP].
 * Returns false when no order exists. */
bool cbm_member_orders_get(CBMMemberOrders *o, const char *type_qn, int side,
                           const CBMMemberStep **steps, int *count);

/* One lookup. */
typedef struct {
    const CBMTypeRegistry *reg; /* the registry whose members are probed */
    int lang;                   /* CBMLanguage, informative */
    const char *type_qn;        /* the start type (its registered QN) */
    int side;
    const char *name;
    uint8_t member_kind; /* CBMMemberKind; 0 = METHOD */
    const char *after;   /* `super`: start after this type's OWN step */
    int after_side;
    uint8_t overloads; /* CBMOverloadMode */
} CBMMemberQuery;

typedef struct {
    const CBMRegisteredFunc *func; /* a method hit */
    const CBMType *field_type;     /* a field hit */
    const char *owner_qn;          /* the type that declares the member */
    const char *via_qn;            /* MIXIN: the type whose group brought the owner in
                                      (the outermost one); else the owner */
    const char *member_name;       /* the declared name (an alias's target name) */
    uint8_t place;                 /* CBMMemberPlace */
    bool prefix_complete;          /* true = proven: no step before the hit was OUTSIDE or
                                      a SOFT place; false = possible, never a proof */
    uint8_t mixin_depth;           /* sub-orders the hit lies in: 0 own / superclass /
                                      interface, 1 a direct mixin, 2+ a mixin's mixin
                                      (capped at 255) */
    bool renamed;                  /* found under an alias (a rename of a group's holder) */
} CBMMemberHit;

typedef struct {
    uint64_t work; /* steps read, orders built, index entries made: the counted growth unit */
    CBMMemberOrders *orders;
    void *state; /* answers kept across lookups, the frame stack, indexes (member_walk.c) */
    /* COLLECT / HIDE_BY_SIGNATURE: every hit of the last lookup. */
    CBMMemberHit *hits;
    int hit_count;
    int hit_cap;
} CBMMemberWalker;

void cbm_member_walker_init(CBMMemberWalker *w, CBMMemberOrders *orders);
void cbm_member_walker_free(CBMMemberWalker *w);

/* Look the member up. *out receives the hit on FOUND (the first one for the
 * collecting modes, whose full list is w->hits[0, w->hit_count)). */
CBMMemberWalkResult cbm_member_lookup(CBMMemberWalker *w, const CBMMemberQuery *q,
                                      CBMMemberHit *out);

/* ── Linearisers ─────────────────────────────────────────────────────
 *
 * cbm_lin_own_traits_parent (PHP): own members; the traits as one exclusive
 * GROUP of sub-orders (renames of the using type applied); the interfaces
 * (IMPLEMENTS, and an interface's EXTENDS) as LATER steps; then the parent
 * (the one EXTENDS of a class) by NEXT. A supertype that is not KNOWN becomes
 * a SOFT unknown place where it stands (a group member, an unknown LATER, an
 * UNSEEN followed by STOP in place of the parent; so do two parents): what
 * lies behind it answers only as a possible hit, and nothing is ever absent.
 * OPEN after the own step for CBM_MEMBERS_OPEN; a hard UNSEEN after it for
 * COMPUTED and REOPENED_EXTERNAL; DECLS_DISAGREE makes the order [OWN, UNSEEN].
 * A class with no parent ends with STOP. */
bool cbm_lin_own_traits_parent(CBMMemberOrders *o, const CBMRegisteredType *t, int side,
                               CBMMemberSteps *out);

#endif /* CBM_LSP_MEMBER_WALK_H */
