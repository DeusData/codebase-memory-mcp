#ifndef CBM_STORE_RESOLVER_EVIDENCE_H
#define CBM_STORE_RESOLVER_EVIDENCE_H
#include "resolve/record.h"
#include <sqlite3.h>
/* Called inside the writer's savepoint. All roots must be owned by one canonical
 * generation DAG; the caller's graph stays alive through write. With commit_batches
 * the savepoint is released and reopened as rows accumulate, so the WAL can reset:
 * only for a private store that a failure discards, since the rows written so far
 * then survive a failed write and only the missing receipt says so. */
bool cbm_store_resolver_evidence(sqlite3 *db, const char *project, const cbm_resolve_record_t *rows,
                                 size_t count, const cbm_resolve_file_t *files, size_t file_count,
                                 const cbm_resolve_summary_t *summaries, size_t summary_count,
                                 bool commit_batches);
/* True when sql, as stored in sqlite_schema, is exactly how the writer defines this
 * logical view. A view of another name or definition is not this generation's data. */
bool cbm_store_resolver_view_definition_current(const char *sql);
#if defined(CBM_ENABLE_TEST_SEAMS) && CBM_ENABLE_TEST_SEAMS
/* Rows between progress lines; 0 restores the production interval. */
void cbm_store_resolver_test_set_progress_rows(uint64_t rows);
/* Digests the last evidence write kept in memory. */
size_t cbm_store_resolver_test_held_digests(void);
#endif
#endif
