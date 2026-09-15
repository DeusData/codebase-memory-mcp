/*
 * result_spill.h — per-file extraction results parked on disk when the
 * memory budget is hit.
 *
 * A compacted CBMFileResult is one exact-size arena block plus a header whose
 * pointers all point into that block (cbm_result_compact guarantees it). That
 * makes a result a relocatable blob: written as header + block, read back at
 * any address by shifting every pointer by the base delta. The store is one
 * append-only file per writer thread under the cache directory; loads use
 * pread and are safe from any thread. Nothing here decides WHEN to spill --
 * the pipeline does, on the budget signal (see cbm_pipeline_ctx_t.spill).
 */
#ifndef CBM_RESULT_SPILL_H
#define CBM_RESULT_SPILL_H

#include "cbm.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

typedef struct cbm_result_spill cbm_result_spill_t;

/* One store for one pipeline run: `writers` append-only files under
 * <dir>/spill-<pid>-<w>.bin, `slots` result slots (one per input file).
 * NULL on failure (no directory, no space): the caller keeps results in
 * memory as before. */
cbm_result_spill_t *cbm_result_spill_open(const char *dir, int writers, int slots);

/* Park a compacted result in slot `slot` via writer `writer`, then free the
 * in-memory result (a retained parse tree goes with it; a loaded result has
 * none). Returns false (and leaves the result untouched) when the result is
 * not a single-block compaction, owns sub-results, or the write fails. */
bool cbm_result_spill_park(cbm_result_spill_t *sp, int writer, int slot, CBMFileResult *result);

/* True when slot `slot` holds a parked result. */
bool cbm_result_spill_has(const cbm_result_spill_t *sp, int slot);

/* Load slot `slot` back into memory: a fresh CBMFileResult the caller owns
 * (cbm_free_result). NULL when the slot is empty or the read fails. Safe from
 * several threads for different slots. */
CBMFileResult *cbm_result_spill_load(const cbm_result_spill_t *sp, int slot);

/* Read only the parked header of slot `slot` into *out: every count in it
 * is valid, every pointer meaningless. False when the slot is empty or the
 * read fails. Lets a consumer skip the load when there is nothing to read. */
bool cbm_result_spill_peek_header(const cbm_result_spill_t *sp, int slot, CBMFileResult *out);

/* Read only the parked header of slot `slot`: how many defs and impl
 * relations it holds (the collector sizes its array before loading). */
void cbm_result_spill_peek_counts(const cbm_result_spill_t *sp, int slot, int *defs, int *impls);

/* Counters for the log: results parked, bytes on disk, loads served. */
void cbm_result_spill_stats(const cbm_result_spill_t *sp, int64_t *parked, int64_t *bytes,
                            int64_t *loads);

/* Close and delete the files. */
void cbm_result_spill_close(cbm_result_spill_t *sp);

/* Relocation primitive used by the loader: every pointer inside `result`
 * that points into [old_base, old_base + len) is shifted to the block now at
 * `new_base`. Exposed for tests. */
void cbm_result_relocate(CBMFileResult *result, const char *old_base, size_t len, char *new_base);

#endif /* CBM_RESULT_SPILL_H */
