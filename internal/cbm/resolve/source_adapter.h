#ifndef CBM_RESOLVE_SOURCE_ADAPTER_H
#define CBM_RESOLVE_SOURCE_ADAPTER_H

#include "../cbm.h"
#include "model.h"
#include "foundation/hash_table.h"
#include <stdbool.h>
#include <stddef.h>

/* Source adapters for languages whose binding rules are stated directly over
 * their syntax, with no hybrid engine producing evidence rows. Each language
 * supplies its own rules in its own adapter_<lang>.c; this layer only owns the
 * exact-site table and the immutable project declaration table they share.
 *
 * Phase one records what a file declares. Phase two, with every declaration of
 * the project known, states per occurrence which declaration its language's
 * rule selects, or why it abstains. A name that merely happens to be unique in
 * the project selects nothing: a target is reached only through the lookup
 * domain the language itself defines (module, namespace, package, file). */

typedef struct cbm_source_table cbm_source_table_t;
typedef struct cbm_source_unit cbm_source_unit_t;

typedef enum {
    CBM_SOURCE_SITE_CALL = 0, /* invocation occurrence */
    CBM_SOURCE_SITE_REFERENCE /* value or type mention occurrence */
} cbm_source_site_kind_t;

typedef struct {
    const char *target;      /* exact graph QN, NULL when abstaining */
    cbm_resolve_rule_t rule; /* the language rule that selected it */
    const char *domain;      /* lookup domain the rule consulted */
    const char *reason;      /* stable decision or abstention reason */
    uint32_t facts;          /* facts beyond the rule's own, such as a class call */
} cbm_source_outcome_t;

bool cbm_source_adapter_supported(CBMLanguage language);
/* All clauses and arities of one name share one graph node in this language, so
 * several same-QN definitions in one file are that one name, not a conflict. */
bool cbm_source_adapter_one_name_one_node(CBMLanguage language);
/* Relations this language's adapter actually decides. Others stay unsupported. */
bool cbm_source_adapter_decides_references(CBMLanguage language);
/* This language keeps older binding rules, which decide what its adapter leaves. */
bool cbm_source_adapter_has_binding_rules(CBMLanguage language);

cbm_source_table_t *cbm_source_table_new(void);
void cbm_source_table_free(cbm_source_table_t *table);
/* A key to a package's import root, from the project's manifests. Borrowed; must
 * outlive the table. NULL means no package is known. */
void cbm_source_table_set_packages(cbm_source_table_t *table, const CBMHashTable *packages);

/* Phase one. Borrows nothing after return. A file that cannot be analyzed adds
 * no declarations and is reported through cbm_source_table_incomplete. */
bool cbm_source_collect(cbm_source_table_t *table, const CBMFileResult *result,
                        CBMLanguage language, const char *file, const char *source,
                        size_t source_len);
bool cbm_source_table_incomplete(const cbm_source_table_t *table, CBMLanguage language);

/* The QN of a declared type's base class by this language's rules, for the graph's
 * inheritance edges. offset counts the bases this language's rules can name; NULL means
 * there are no more. */
const char *cbm_source_table_base(const cbm_source_table_t *table, CBMLanguage language,
                                  const char *qualified_name, size_t offset);

/* Phase two. The table is read only and may be shared by parallel workers.
 * NULL only on allocation failure; an unanalyzable file yields a unit whose
 * sites all abstain with its failure reason. */
cbm_source_unit_t *cbm_source_unit_build(CBMArena *arena, const cbm_source_table_t *table,
                                         const CBMFileResult *result, CBMLanguage language,
                                         const char *file, const char *source, size_t source_len);

/* Occurrences this language's rules prove at sites extraction has no carrier for, such as
 * Ruby's bare method name, a Ruby constant or a call deep in a Dart selector chain. scope
 * is the enclosing definition's QN, empty for the file. kind says whether the site calls
 * or mentions its target, line is the site's line when the adapter stated one, else 0.
 * Only decided sites are listed. */
typedef struct {
    const char *scope;
    const char *name;
    uint32_t start, end;
    cbm_source_site_kind_t kind;
    uint32_t line;
} cbm_source_extra_call_t;
const cbm_source_extra_call_t *cbm_source_unit_extra_calls(const cbm_source_unit_t *unit,
                                                           size_t *count);

/* Exact occurrence lookup, independent of the extractor's spelling. Always
 * fills out; out->target is NULL for every abstention. */
void cbm_source_unit_site(const cbm_source_unit_t *unit, uint32_t start, uint32_t end,
                          cbm_source_site_kind_t kind, cbm_source_outcome_t *out);

#endif
