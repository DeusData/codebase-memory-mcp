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

/* Initialize memory budget = ram_fraction * total_physical_ram.
 * The CBM_MAX_MEMORY_MB env var, when set to a positive integer, overrides
 * this with an explicit budget in MiB (clamped to physical/cgroup RAM).
 * Thread-safe: only the first call takes effect.
 * Configures mimalloc options for reduced upfront memory. */
void cbm_mem_init(double ram_fraction);

/* Pure budget resolver shared by cbm_mem_init (exposed for testing).
 * Returns ram_fraction * total_ram, unless `max_memory_mb` is a positive
 * integer string (the CBM_MAX_MEMORY_MB override) — then it returns that many
 * MiB, clamped to total_ram when total_ram > 0. Invalid / non-positive
 * overrides fall back to the fraction-derived value. Reads no globals/env. */
size_t cbm_mem_resolve_budget(size_t total_ram, double ram_fraction, const char *max_memory_mb);

/* Current RSS in bytes via mi_process_info().
 * Falls back to OS-specific queries when MI_OVERRIDE=0 (ASan builds). */
size_t cbm_mem_rss(void);

/* Peak RSS in bytes. */
size_t cbm_mem_peak_rss(void);

/* Total budget in bytes. */
size_t cbm_mem_budget(void);

/* Returns true if current RSS exceeds the budget. */
bool cbm_mem_over_budget(void);

/* Per-worker budget hint: budget / num_workers. */
size_t cbm_mem_worker_budget(int num_workers);

/* Return unused pages to the OS. Call between files to bound per-file peak. */
void cbm_mem_collect(void);

#endif /* CBM_MEM_H */
