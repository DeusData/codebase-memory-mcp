/*
 * mem.h — Unified memory management via mimalloc.
 *
 * Provides budget tracking based on actual RSS (not partial vmem tracking).
 * Uses mi_process_info() as the single source of truth for memory pressure.
 * Replaces the old vmem.h budget-tracked virtual memory allocator.
 */
#ifndef CBM_MEM_H
#define CBM_MEM_H

#include <stdbool.h>
#include <stddef.h>

/* Tiered default fraction for MCP startup: 25% on <=16GB, 35% on <=32GB, else 50%. */
double cbm_mem_ram_fraction_for_total(size_t total_ram_bytes);

/* Initialize memory budget = ram_fraction * total_physical_ram.
 * The CBM_MEM_BUDGET_MB env var, when set to a positive integer, overrides
 * this with an explicit budget in MiB (clamped to physical/cgroup RAM).
 * Thread-safe: only the first call takes effect.
 * Configures mimalloc options for reduced upfront memory. */
void cbm_mem_init(double ram_fraction);

/* Worker-only initialization cap carried on the build-bound internal argv.
 * The existing user override is still resolved first; a lower explicit
 * CBM_MEM_BUDGET_MB wins, while a larger/default budget is capped. */
void cbm_mem_init_with_cap(double ram_fraction, size_t hard_cap_bytes);

/* Result of cbm_mem_resolve_budget: the resolved budget plus the metadata
 * cbm_mem_init logs — so the parse/clamp logic lives in exactly ONE place and
 * the caller never re-parses the env string. */
typedef struct {
    size_t budget;      /* resolved budget in bytes */
    const char *source; /* "ram_fraction" | "CBM_MEM_BUDGET_MB" | "daemon_worker_cap" */
    bool clamped;       /* override was valid but exceeded total_ram → clamped down */
    bool invalid;       /* override was present but unparseable / out-of-range / ≤0 */
    bool hard_capped;   /* internal worker hard cap reduced the resolved budget */
} cbm_mem_budget_t;

/* Pure budget resolver shared by cbm_mem_init (exposed for testing).
 * Returns ram_fraction * total_ram, unless `budget_mb` is a STRICTLY valid
 * positive integer string (the CBM_MEM_BUDGET_MB override) — then it returns
 * that many MiB, clamped to total_ram when total_ram > 0. Trailing garbage,
 * overflow (ERANGE), and non-positive values are rejected (invalid=true) and
 * fall back to the fraction-derived value. Reads no globals/env. */
cbm_mem_budget_t cbm_mem_resolve_budget(size_t total_ram, double ram_fraction,
                                        const char *budget_mb);

/* Pure capped variant used by supervised workers and deterministic tests. */
cbm_mem_budget_t cbm_mem_resolve_budget_capped(size_t total_ram, double ram_fraction,
                                               const char *budget_mb, size_t hard_cap_bytes);

/* Current RSS in bytes via mi_process_info().
 * Falls back to OS-specific queries when MI_OVERRIDE=0 (ASan builds). */
size_t cbm_mem_rss(void);

/* Peak RSS in bytes. */
size_t cbm_mem_peak_rss(void);

/* Total budget in bytes. */
size_t cbm_mem_budget(void);

/* TEST HOOK: overwrite the budget directly, bypassing cbm_mem_init's
 * init-once guard (a setenv+re-init dance in tests is a silent no-op once
 * some earlier init won the guard — the poisoned budget then leaks into
 * every later budget consumer in the process). Does NOT flip the init
 * guard: a later cbm_mem_init still initializes normally. Callers must
 * save cbm_mem_budget() first and restore it before their assertions.
 * Never call from production code. */
void cbm_mem_set_budget_for_tests(size_t bytes);

/* Returns true if current RSS exceeds the budget. */
bool cbm_mem_over_budget(void);

/* Per-worker budget hint: budget / num_workers. */
size_t cbm_mem_worker_budget(int num_workers);

/* Return unused pages to the OS. Call between files to bound per-file peak. */
void cbm_mem_collect(void);

/* ── Memory map: where does the process's memory actually live? ──────
 *
 * A growth diagnostic that is honest about what it cannot see. Walking the
 * allocator reports live bytes for the CALLING thread's heap only, so a leak
 * on another thread — or memory never routed through the allocator — would be
 * invisible to a naive per-subsystem tally. Every consumer therefore gets
 * three independent totals plus an explicit residual, so an incomplete map
 * announces itself instead of looking balanced:
 *
 *   os_committed_bytes  what the OS charges the process
 *   live_bytes          allocator blocks live on THIS thread's heap
 *   residual            os_committed - live_bytes (other threads, allocator
 *                       retention, or non-allocator memory)
 *
 * Reading the triple localises growth without guessing:
 *   live grows                → leak on this thread's heap; buckets say which size
 *   residual grows, live flat → another thread's heap, retention, or OS/CRT memory
 *   both flat but RSS grows   → mapped files or stacks, not the heap
 *
 * The bucket histogram splits live bytes by size class, so a distinctive class
 * ("thousands of 384-byte blocks appeared") identifies the allocation without
 * per-callsite instrumentation.
 */
enum { CBM_MEM_MAP_BUCKETS = 8 };

typedef struct {
    /* Is plain malloc actually served by the configured allocator on this
     * build? If false, every allocator tuning in cbm_mem_init (immediate
     * purge, page reclaim, arena policy) is inert for ordinary allocations,
     * the walk below sees nothing, and freed pages stay committed until the
     * platform's own heap decides otherwise. Checked FIRST when reading a
     * map: a false here explains a zero live_bytes, and is a platform-parity
     * defect in its own right (#581 on Windows). */
    bool malloc_is_allocator_owned;
    size_t os_committed_bytes;
    size_t os_rss_bytes;
    size_t live_bytes;
    size_t live_blocks;
    size_t area_committed_bytes;
    size_t area_reserved_bytes;
    /* Live bytes by size class: <=64, <=256, <=1K, <=4K, <=16K, <=64K, <=1M, rest */
    size_t bucket_bytes[CBM_MEM_MAP_BUCKETS];
    size_t bucket_blocks[CBM_MEM_MAP_BUCKETS];
} cbm_mem_map_t;

/* Inclusive upper bound of each size class; the last bucket is open-ended and
 * reports 0. */
size_t cbm_mem_map_bucket_limit(int bucket);

/* Snapshot the current memory map. Returns false only when out is NULL. If the
 * allocator declines the walk, the OS totals are still filled and live_bytes
 * stays 0, so the residual carries the whole process and the caller can SEE
 * that the walk contributed nothing rather than reading 0 as "no leak". */
bool cbm_mem_map_collect(cbm_mem_map_t *out);

/* OS totals only: fills os_rss_bytes / os_committed_bytes and leaves the
 * allocator walk out entirely, so live_bytes stays 0 and the residual carries
 * the whole process.
 *
 * Required for anything running on a background thread. Reading the
 * allocator's per-thread bookkeeping (even just mi_theap_get_default) races a
 * thread exiting concurrently, which rewrites it from _mi_thread_done ->
 * _mi_theap_default_set; TSan reports that pairing directly. The walk is only
 * safe where no other thread can be exiting -- tests and explicitly requested
 * diagnostics -- so the periodic snapshot uses this instead. */
bool cbm_mem_map_collect_os(cbm_mem_map_t *out);

/* ── Allocator ownership audit ───────────────────────────────────────
 *
 * "Does the allocator own our memory" is not one question, it is one per size
 * class. An override can capture small allocations while large or huge ones go
 * elsewhere, and the predicate used to route frees can itself be wrong for a
 * class — which silently hands real allocator blocks to the wrong deallocator.
 * That is a leak at best and undefined behaviour at worst, so it has to be
 * measured across the size range rather than probed once.
 *
 * Verified at startup and assertable in tests, so an unowned class is a loud
 * failure instead of a slow ratchet nobody attributes for months (#581).
 */
enum { CBM_MEM_OWNERSHIP_CLASSES = 6 };

typedef struct {
    size_t probe_bytes[CBM_MEM_OWNERSHIP_CLASSES]; /* size probed per class */
    bool owned[CBM_MEM_OWNERSHIP_CLASSES];         /* predicate said "ours" */
    bool allocated[CBM_MEM_OWNERSHIP_CLASSES];     /* the probe itself worked */
    int owned_count;
    int probed_count;
    bool all_owned; /* every successfully probed class came back owned */
} cbm_mem_ownership_audit_t;

/* Allocate one block per size class, ask the routing predicate whether the
 * allocator owns it, then free it. Pure measurement: mutates no globals. */
void cbm_mem_audit_ownership(cbm_mem_ownership_audit_t *out);

/* Foreign-pointer accounting from the allocation interposer: how many pointers
 * reaching our deallocators were classified as NOT allocator-owned. On a
 * correctly wired build this stays at or near zero. A climbing count means
 * either an un-intercepted allocation path or — as seen while landing the
 * Windows interposer — a predicate misclassifying real allocator blocks.
 * Either way it is a defect signal, which is why soak/smoke can gate on it.
 * Both read zero on platforms with no interposer. */
size_t cbm_mem_foreign_pointer_count(void);
size_t cbm_mem_owned_pointer_count(void);

/* Interposer hook: classify one pointer arriving at a deallocator. */
void cbm_mem_record_pointer_ownership(bool owned);

/* ── Phase map: WHICH code path does the memory stay in? ─────────────
 *
 * The allocator walk answers "how much and what size"; it cannot answer "who".
 * When the growth does not route through the allocator at all — the Windows
 * case in #581 — attribution has to come from the one metric that does see it:
 * OS committed bytes. Marks bracket a critical path, and each mark attributes
 * the committed-bytes delta since the previous mark to the previous label. Over
 * many requests the label whose total climbs monotonically is where memory
 * stays.
 *
 * Deliberately coarse and honest about it:
 *   - OS committed is process-wide, so a concurrently-busy thread lands noise
 *     in whichever label is open. Signal comes from ACCUMULATION over many
 *     passes, never from a single delta.
 *   - Marks must bracket the WHOLE path with no unlabelled gaps, or the leak
 *     hides in the gap. Label the tail explicitly.
 *   - Off unless CBM_MEM_PHASES=1, so the hot path pays one atomic load.
 */
void cbm_mem_phase_mark(const char *label);

/* Drop all accumulated phase totals (call once at the start of a measurement). */
void cbm_mem_phase_reset(void);

/* Write the phase table as a JSON array of {label, bytes, hits}, biggest total
 * first. Returns bytes written (0 when disabled or empty). */
int cbm_mem_phase_report_json(char *out, size_t size);

/* ── Windows region census: which POOL holds the memory? ─────────────
 *
 * The allocator walk can only describe memory the allocator owns. When the
 * process commits far more than the allocator holds — 440 MB private against
 * 34.7 MiB of arenas (#581) — the remainder is by definition somewhere else,
 * and "somewhere else" has to be named before it can be fixed. Guessing
 * between the CRT heap, private VirtualAlloc, mapped files and thread stacks
 * costs a soak run per guess; asking the OS costs one pass.
 *
 * Two independent walks, deliberately overlapping so they cross-check:
 *   VirtualQuery over the whole address space  → committed bytes by region TYPE
 *   GetProcessHeaps + HeapWalk                 → what the CRT heaps hold
 *
 * committed_private is the total the OS charges us for anonymous memory; the
 * allocator's arenas and the CRT heaps are both drawn from it, so
 *   committed_private - crt_heap_committed - <arena committed>
 * is the unattributed remainder. A census where that remainder is the part
 * that grows says the leak is in neither heap — which is a different bug from
 * either heap leaking, and points at raw VirtualAlloc or stacks instead.
 *
 * Windows-only; returns false everywhere else so callers must handle absence
 * rather than silently reading zeros as "nothing there".
 */
enum { CBM_MEM_TRACKED_REGIONS = 6 };

typedef struct {
    size_t committed_total;    /* all MEM_COMMIT regions */
    size_t committed_private;  /* MEM_PRIVATE: heaps, stacks, raw VirtualAlloc */
    size_t committed_mapped;   /* MEM_MAPPED: file/section views */
    size_t committed_image;    /* MEM_IMAGE: loaded modules */
    size_t reserved_total;     /* MEM_RESERVE, uncommitted */
    size_t crt_heap_committed; /* committed bytes across all CRT heaps */
    size_t crt_heap_busy;      /* bytes in live allocations inside them */
    unsigned crt_heap_count;
    unsigned regions;
    /* The single largest committed private region, and how many are >=1MiB.
     * One enormous region means one reservation (allocator metadata, a pagemap)
     * rather than many leaked blocks — a different bug with a different fix. */
    /* Private commit split by region size. Growth invariant to every allocator
     * option means the memory is not the allocator's; the size profile of what
     * accumulates says what it IS — a swarm of small commits, mid-sized blocks,
     * or thread-stack-sized reservations. */
    size_t private_small_bytes;  /* regions < 64 KiB */
    size_t private_medium_bytes; /* 64 KiB .. 1 MiB */
    size_t private_large_bytes;  /* >= 1 MiB */
    unsigned private_small_count;
    unsigned private_medium_count;
    unsigned private_large_count;
    /* Identity, not just size: base and extent of the largest private regions,
     * so they can be matched against the allocator's arena addresses instead of
     * guessed at by toggling options one at a time. */
    void *large_base[CBM_MEM_TRACKED_REGIONS];
    size_t large_size[CBM_MEM_TRACKED_REGIONS];
    unsigned large_tracked;
    size_t largest_private_region;
    unsigned private_regions_1mb;
    /* Base of that region: a base that stays put while the size climbs proves
     * ONE allocation growing, and lets it be matched against the allocator's
     * arena addresses to name the owner. */
    void *largest_private_base;
    bool heap_walk_ok; /* false → crt_* are unknown, not zero */
} cbm_mem_win_census_t;

/* Snapshot the OS-level region census. Returns false when unavailable
 * (non-Windows, or out is NULL). */
bool cbm_mem_win_census(cbm_mem_win_census_t *out);

/* Emit one census line tagged with the call site, so a growth series can be
 * read straight out of the daemon log. No-op unless CBM_MEM_CENSUS=1. */
void cbm_mem_census_log(const char *tag);

/* Census logging gate: CBM_MEM_CENSUS=1. Off by default — HeapWalk locks each
 * CRT heap, so this is a diagnostic, never a hot-path metric. */
bool cbm_mem_census_enabled(void);

#endif /* CBM_MEM_H */
