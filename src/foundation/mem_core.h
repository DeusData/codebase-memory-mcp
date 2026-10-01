/*
 * mem_core.h — THE allocation route.
 *
 * Memory in this project is allocated through one core, not through ~800
 * scattered malloc/calloc/strdup sites. This header is that core;
 * foundation/mem.h remains policy and measurement (budget, RSS, pressure,
 * phase marks) and deliberately owns no allocation.
 *
 * WHY THIS EXISTS (audited 2026-09-13)
 *
 * The budget could only ever be OBSERVED, never enforced. cbm_mem_over_budget()
 * reads process RSS *after* the allocation that crossed the line already
 * succeeded, and the only available response was to nap. A thermostat wired to
 * a thermometer with no cooler attached.
 *
 * Worse, we could not even say WHERE the memory was. Two independent reasons:
 *
 *   1. cbm_mem_map_collect()'s live_bytes walks mi_theap_get_default() — THIS
 *      thread's mimalloc heap only. Walking the process-wide mi_heap_main() is
 *      a data race that TSan caught on macOS, so it is deliberately not done.
 *      An 18-worker index therefore attributes almost nothing; the rest lands
 *      in `residual`.
 *   2. The mimalloc global override is ON for Linux/MinGW and permanently OFF
 *      for macOS (the two-level namespace turns this binary's free into mi_free
 *      while system libraries keep allocating from the system zone, and a
 *      pointer crossing that boundary aborts). On macOS ordinary malloc is
 *      served by the SYSTEM allocator: the startup audit reports
 *      owned_classes=0/6. Any accounting that assumes mimalloc owns the pointer
 *      is blind on an entire platform.
 *
 * So the core keeps its OWN counters. Atomic, per class, incremented at
 * allocation and decremented at free. Thread-safe by construction, identical on
 * every platform, and independent of which allocator actually serves malloc.
 *
 * NO PER-ALLOCATION HEADER. The obvious design — a {class,size} prefix — costs
 * 16 bytes on every block, and the graph buffer makes ~100M of them at kernel
 * scale: ~1.6 GB of pure overhead to measure a memory problem. Sizes come from
 * the platform's usable-size query instead, which is exact-to-the-bucket, free,
 * and correct under either allocator.
 */
#ifndef CBM_MEM_CORE_H
#define CBM_MEM_CORE_H

#include <stdbool.h>
#include <stddef.h>

/* Allocation classes.
 *
 * A class is a BUDGETING bucket, not a type taxonomy: split only where the
 * split would change a decision. These follow the measured phase profile of a
 * kernel index (2026-09-13), where the peak was 35.20 GB and the two consumers
 * behaved differently — extraction transients scale with worker count, the
 * semantic plateau does not. Attribution that cannot separate those two cannot
 * choose between "park workers" and "stream the vectors". */
typedef enum {
    CBM_MEM_CLASS_OTHER = 0,   /* unclassified; the residual to drive down */
    CBM_MEM_CLASS_GBUF_NODE,   /* node records (~64 B each) */
    CBM_MEM_CLASS_GBUF_EDGE,   /* edge records (~48 B each) */
    CBM_MEM_CLASS_GBUF_STRING, /* name / qualified_name / properties_json */
    CBM_MEM_CLASS_GBUF_INDEX,  /* the 8 lookup indexes (383 MB peak on the Go corpus) */
    CBM_MEM_CLASS_EXTRACT,     /* per-file working set: source text, extraction scratch */
    CBM_MEM_CLASS_ARENA,       /* CBMArena blocks -- every arena, whoever owns it */
    CBM_MEM_CLASS_TS_TREE,     /* tree-sitter: parse trees + parser state (bound allocator) */
    CBM_MEM_CLASS_SEMANTIC,    /* semantic pass: vectors, token pools, LSH (87 MB on Go) */
    CBM_MEM_CLASS_DUMP,        /* dump-time transients */
    CBM_MEM_CLASS_STORE,       /* SQLite (bound mem methods) + store batches and row buffers */
    CBM_MEM_CLASS_HASH_TABLE,  /* CBMHashTable buckets/entries not claimed by an owner class */
    CBM_MEM_CLASS_DYN_ARRAY,   /* CBM_DYN_ARRAY item storage (every cbm_da_* user) */
    CBM_MEM_CLASS_COUNT
} cbm_mem_class_t;

/* Stable lowercase name, for logs and JSON. Never NULL, even out of range. */
const char *cbm_mem_class_name(cbm_mem_class_t cls);

/* ── The allocation route ──────────────────────────────────────────────
 *
 * Semantics match the C library exactly, so adoption is a mechanical rename and
 * never a behaviour change:
 *   - cbm_alloc(cls, 0) returns a non-NULL pointer that is valid to free
 *   - cbm_free(cls, NULL) is a no-op
 *   - cbm_realloc(cls, NULL, n) behaves as cbm_alloc
 *   - a failed realloc leaves the original block intact and returns NULL
 *
 * The class passed to free/realloc MUST be the class the block was allocated
 * with, or the counters drift. Pass the class through alongside the pointer,
 * the same way a custom deleter would. */
void *cbm_alloc(cbm_mem_class_t cls, size_t bytes);
void *cbm_calloc(cbm_mem_class_t cls, size_t bytes);
void *cbm_realloc(cbm_mem_class_t cls, void *block, size_t bytes);
char *cbm_mem_strdup(cbm_mem_class_t cls, const char *s);
void cbm_free(cbm_mem_class_t cls, void *block);

/* ── Accounting ────────────────────────────────────────────────────────
 *
 * live_bytes is what the ALLOCATOR handed us (usable size), so it exceeds the
 * bytes requested by the per-block rounding and is the honest number for a
 * memory budget: rounding is memory the process cannot use for anything else.
 *
 * These counters see only memory that went through this core. Everything still
 * on raw malloc is invisible here — which is the point of
 * cbm_mem_tracked_live_bytes() versus the process RSS in mem.h: the gap between
 * them IS the unmigrated surface, and it should shrink as adoption spreads. An
 * unmeasured allocation must never read as an absent one. */
/* Push this thread's pending accounting deltas to the shared counters. Every
 * reader does it for its own thread; a parallel-for worker does it when its
 * work item ends, so the phase marks (read after the join) are exact. */
void cbm_mem_class_flush_thread(void);

size_t cbm_mem_class_live_bytes(cbm_mem_class_t cls);
size_t cbm_mem_class_live_blocks(cbm_mem_class_t cls);
size_t cbm_mem_tracked_live_bytes(void);

/* Peak live bytes for a class since process start (or the last reset). The
 * budget question is always about the PEAK, never the value at the moment
 * someone happened to look. */
size_t cbm_mem_class_peak_bytes(cbm_mem_class_t cls);

/* Drop all peaks to the current live values. For a measurement run that wants
 * one phase, not the whole process history. Never resets live counters --
 * those track real outstanding blocks. */
void cbm_mem_class_reset_peaks(void);

/* JSON array of {class, live_bytes, live_blocks, peak_bytes}, biggest live
 * first, classes with no activity omitted. Returns bytes written, 0 if none. */
int cbm_mem_class_report_json(char *out, size_t size);

/* Log the class table at info level under `tag`. For phase boundaries in the
 * index pipeline, where the interesting question is which class grew. */
void cbm_mem_class_log(const char *tag);

/* Usable size of a block obtained from this core, or 0 when the platform
 * cannot answer. Exposed because the same query is what makes header-free
 * accounting possible, and callers doing their own bulk accounting (arenas)
 * need the identical definition to stay consistent with these counters. */
size_t cbm_mem_usable_size(const void *block);

/* ── Bulk accounting, for allocators that are not this one ─────────────
 *
 * The extraction engine already allocates through arenas (1301 call sites) and
 * must NOT be rewritten to per-object cbm_alloc — that would undo the very
 * batching that keeps its allocation count low. Instead an arena reports its
 * block acquisitions here, so arena-backed memory appears in the same table as
 * heap memory and the totals stay comparable.
 *
 * Symmetric: every add must be matched by a remove of the same size. */
void cbm_mem_class_add_external(cbm_mem_class_t cls, size_t bytes);
void cbm_mem_class_remove_external(cbm_mem_class_t cls, size_t bytes);

#endif /* CBM_MEM_CORE_H */
