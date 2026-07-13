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

/* Result of cbm_mem_resolve_budget: the resolved budget plus the metadata
 * cbm_mem_init logs — so the parse/clamp logic lives in exactly ONE place and
 * the caller never re-parses the env string. */
typedef struct {
    size_t budget;      /* resolved budget in bytes */
    const char *source; /* log token: "ram_fraction" | "CBM_MEM_BUDGET_MB" */
    bool clamped;       /* override was valid but exceeded total_ram → clamped down */
    bool invalid;       /* override was present but unparseable / out-of-range / ≤0 */
} cbm_mem_budget_t;

/* Pure budget resolver shared by cbm_mem_init (exposed for testing).
 * Returns ram_fraction * total_ram, unless `budget_mb` is a STRICTLY valid
 * positive integer string (the CBM_MEM_BUDGET_MB override) — then it returns
 * that many MiB, clamped to total_ram when total_ram > 0. Trailing garbage,
 * overflow (ERANGE), and non-positive values are rejected (invalid=true) and
 * fall back to the fraction-derived value. Reads no globals/env. */
cbm_mem_budget_t cbm_mem_resolve_budget(size_t total_ram, double ram_fraction,
                                        const char *budget_mb);

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

#endif /* CBM_MEM_H */
