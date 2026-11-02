#ifndef CBM_LSP_OO_STRICT_H
#define CBM_LSP_OO_STRICT_H

#include "../cbm.h"
#include <stdbool.h>

/* Strict declaration-selection adapters for Java, Kotlin and C#.
 *
 * A project-wide symbol index is harvested once from source syntax. Every
 * file is then walked with that language's lexical scopes and static member
 * lookup. A row is produced only where the language's own rules select one
 * declaration, and it joins the extraction carrier at the same byte span:
 * CALLS names the declaration chosen statically, never a runtime target.
 *
 * Unknown stays unknown. An unresolved supertype, an unmodelled binding form
 * or competing declarations withhold the row instead of narrowing a guess. */
typedef struct cbm_oo_index cbm_oo_index_t;

bool cbm_oo_language(CBMLanguage language);

/* The arena must outlive the index and every row a resolve borrows from it. */
cbm_oo_index_t *cbm_oo_index_new(CBMArena *arena);
/* Coordinator only: one file at a time, before seal. */
bool cbm_oo_index_add_file(cbm_oo_index_t *index, CBMLanguage language, const char *rel_path,
                           const CBMFileResult *file, const char *source, int source_len);
/* Coordinator only, between files: every later file belongs to the build project in this
 * directory (a .csproj, build.gradle or pom.xml beside it or above it). NULL: no known project. */
void cbm_oo_index_set_project(cbm_oo_index_t *index, const char *project_dir);
/* Coordinator only, between files: every later file is a reference assembly's
 * source. Such a declaration is the contract of the implementation of its name,
 * which stands for it. Call it after the implementation files are in. */
void cbm_oo_index_begin_reference_files(cbm_oo_index_t *index);
/* Resolves supertypes. After this the index is immutable and shared by readers. */
void cbm_oo_index_seal(cbm_oo_index_t *index);
void cbm_oo_index_free(cbm_oo_index_t *index);

/* Appends rows to out. Allocations come from arena; the index is only read. */
void cbm_oo_resolve_file(const cbm_oo_index_t *index, CBMArena *arena, CBMLanguage language,
                         const char *rel_path, const char *file_qn, const char *source,
                         int source_len, TSTree *tree, const CBMFileResult *file,
                         CBMResolvedCallArray *out);

#endif
