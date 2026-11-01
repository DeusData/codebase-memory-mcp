#ifndef CBM_STORE_RESOLVER_EVIDENCE_H
#define CBM_STORE_RESOLVER_EVIDENCE_H
#include "resolve/record.h"
#include <sqlite3.h>
/* Called inside the occurrence writer's savepoint. All roots must be owned by
 * one canonical generation DAG; the caller's graph stays alive through write. */
bool cbm_store_resolver_evidence(sqlite3 *db, const char *project, const cbm_resolve_record_t *rows,
                                 size_t count, const cbm_resolve_file_t *files, size_t file_count,
                                 const cbm_resolve_summary_t *summaries, size_t summary_count);
#if defined(CBM_ENABLE_TEST_SEAMS) && CBM_ENABLE_TEST_SEAMS
/* Rows between progress lines; 0 restores the production interval. */
void cbm_store_resolver_test_set_progress_rows(uint64_t rows);
#endif
#endif
