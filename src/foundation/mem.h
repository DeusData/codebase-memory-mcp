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
 * Thread-safe: only the first call takes effect.
 * Configures mimalloc options for reduced upfront memory. */
void cbm_mem_init(double ram_fraction);

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
