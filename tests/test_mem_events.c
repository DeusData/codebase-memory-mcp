/*
 * test_mem_events.c — the allocation-event layer of the waste sanitizer.
 *
 * The layer never dereferences a block, so these tests feed it synthetic
 * addresses: every assertion is about ACCOUNTING, and the verdicts are pure
 * functions of the event sequence (O9) — no allocator, no clock, no timing.
 */
#include "test_framework.h"
#include "../src/foundation/compat.h"
#include "../src/foundation/compat_fs.h"
#include "../src/foundation/compat_thread.h"
#include "../src/foundation/mem_events.h"

#include <stdint.h>
#include <stdio.h>
#include <string.h>
#ifndef _WIN32
#include <unistd.h>
#else
#include <process.h>
#define getpid _getpid
#endif

#define BLK(n) ((void *)(uintptr_t)(0x100000u + ((uintptr_t)(n) << 4)))
#define SITE_A ((void *)(uintptr_t)0xA000)
#define SITE_B ((void *)(uintptr_t)0xB000)
#define SITE_C ((void *)(uintptr_t)0xC000)

static void fresh(void) {
    cbm_memev_force_for_tests(true);
    cbm_memev_reset_for_tests();
}

static bool site_row(void *site, cbm_memev_site_t *out) {
    static cbm_memev_site_t rows[64];
    size_t n = cbm_memev_sites(rows, 64);
    for (size_t i = 0; i < n; i++) {
        if (rows[i].site == (uintptr_t)site) {
            *out = rows[i];
            return true;
        }
    }
    return false;
}

/* Requested vs usable is the allocator's slack, and it belongs to the site
 * that asked: 3 x (100 requested, 112 handed out) = 36 bytes nobody can use. */
TEST(memev_slack_is_charged_to_the_asking_site) {
    fresh();
    for (int i = 0; i < 3; i++) {
        cbm_memev_alloc(BLK(i), 100, 112, SITE_A);
    }
    cbm_memev_alloc(BLK(9), 64, 64, SITE_B);
    cbm_memev_site_t a;
    ASSERT_TRUE(site_row(SITE_A, &a));
    ASSERT_EQ(a.allocs, 3);
    ASSERT_EQ(a.requested_bytes, 300);
    ASSERT_EQ(a.usable_bytes, 336);
    ASSERT_EQ(a.live_bytes, 336);
    ASSERT_EQ(a.live_blocks, 3);
    cbm_memev_free(BLK(1));
    ASSERT_TRUE(site_row(SITE_A, &a));
    ASSERT_EQ(a.frees, 1);
    ASSERT_EQ(a.live_bytes, 224);
    ASSERT_EQ(a.live_blocks, 2);
    cbm_memev_totals_t t;
    cbm_memev_totals(&t);
    ASSERT_EQ(t.allocs, 4);
    ASSERT_EQ(t.sites, 2);
    ASSERT_EQ(t.live_bytes, 288);
    ASSERT_EQ(t.untracked_frees, 0);
    PASS();
}

/* A block freed by its own thread within a few allocations is churn: an
 * arena or a stack buffer would have served. One that outlives the window
 * is not. */
TEST(memev_short_lived_blocks_are_counted_as_churn) {
    fresh();
    for (int i = 0; i < 10; i++) {
        cbm_memev_alloc(BLK(i), 32, 32, SITE_A);
        cbm_memev_free(BLK(i));
    }
    cbm_memev_alloc(BLK(100), 32, 32, SITE_B); /* lives across the window */
    for (int i = 0; i < CBM_MEMEV_CHURN_WINDOW + 8; i++) {
        cbm_memev_alloc(BLK(200 + i), 8, 8, SITE_C);
    }
    cbm_memev_free(BLK(100));
    cbm_memev_site_t a;
    cbm_memev_site_t b;
    ASSERT_TRUE(site_row(SITE_A, &a));
    ASSERT_TRUE(site_row(SITE_B, &b));
    ASSERT_EQ(a.short_lived, 10);
    ASSERT_EQ(b.frees, 1);
    ASSERT_EQ(b.short_lived, 0);
    PASS();
}

/* A realloc chain is ONE object growing. The bytes the allocator had to move
 * are that object's cost, charged to the site that created it — not to the
 * realloc call. Growth in place moves nothing. */
TEST(memev_realloc_copy_bytes_go_to_the_original_site) {
    fresh();
    cbm_memev_alloc(BLK(1), 64, 64, SITE_A);
    cbm_memev_realloc(BLK(1), BLK(2), 128, 128, SITE_B); /* moved: 64 bytes copied */
    cbm_memev_realloc(BLK(2), BLK(2), 200, 256, SITE_B); /* in place: nothing copied */
    cbm_memev_realloc(BLK(2), BLK(3), 512, 512, SITE_B); /* moved: 256 bytes copied */
    cbm_memev_site_t a;
    cbm_memev_site_t b;
    ASSERT_TRUE(site_row(SITE_A, &a));
    ASSERT_FALSE(site_row(SITE_B, &b)); /* the realloc call site owns nothing */
    ASSERT_EQ(a.reallocs, 3);
    ASSERT_EQ(a.realloc_copy_bytes, 64 + 256);
    ASSERT_EQ(a.live_blocks, 1);
    ASSERT_EQ(a.live_bytes, 512);
    cbm_memev_free(BLK(3));
    ASSERT_TRUE(site_row(SITE_A, &a));
    ASSERT_EQ(a.live_bytes, 0);
    cbm_memev_totals_t t;
    cbm_memev_totals(&t);
    ASSERT_EQ(t.untracked_frees, 0);
    PASS();
}

/* The memory core announces itself before calling its backing allocator. An
 * observer underneath consumes the hint: one event, the CORE's caller as the
 * site, the class attached. Without an observer the hint stays pending and the
 * core emits the event itself. Either way the block is counted once. */
TEST(memev_hint_gives_one_event_with_the_cores_site_and_class) {
    fresh();
    /* observed path: hint, then the observer's event (which passes its own frame) */
    cbm_memev_hint(SITE_A, 5);
    ASSERT_TRUE(cbm_memev_hint_pending());
    cbm_memev_alloc(BLK(1), 40, 48, SITE_C);
    ASSERT_FALSE(cbm_memev_hint_pending());
    /* unobserved path: nobody consumed it, the core emits */
    cbm_memev_hint(SITE_A, 5);
    if (cbm_memev_hint_pending()) {
        cbm_memev_alloc(BLK(2), 40, 48, NULL);
    }
    /* an allocation that never touched the core */
    cbm_memev_alloc(BLK(3), 40, 48, SITE_B);

    cbm_memev_site_t a;
    cbm_memev_site_t b;
    cbm_memev_site_t c;
    ASSERT_TRUE(site_row(SITE_A, &a));
    ASSERT_TRUE(site_row(SITE_B, &b));
    ASSERT_FALSE(site_row(SITE_C, &c)); /* the observer's frame never becomes a site */
    ASSERT_EQ(a.allocs, 2);
    ASSERT_EQ(a.raw_bytes, 0);  /* tagged by the core */
    ASSERT_EQ(b.raw_bytes, 48); /* the unmigrated surface, as a number */
    cbm_memev_totals_t t;
    cbm_memev_totals(&t);
    ASSERT_EQ(t.raw_bytes, 48);

    /* frees carry no handshake: whoever is responsible reports BEFORE the block goes back */
    cbm_memev_free(BLK(1));
    cbm_memev_free(BLK(2));
    cbm_memev_totals(&t);
    ASSERT_EQ(t.frees, 2);
    ASSERT_EQ(t.untracked_frees, 0);
    PASS();
}

/* A free of a block the layer never saw is a NUMBER, not a silent no-op: it
 * measures how much traffic predates or bypasses the observers. */
TEST(memev_untracked_free_is_counted_not_dropped) {
    fresh();
    cbm_memev_free(BLK(77));
    cbm_memev_free(NULL);
    cbm_memev_totals_t t;
    cbm_memev_totals(&t);
    ASSERT_EQ(t.untracked_frees, 1);
    ASSERT_EQ(t.frees, 0);
    PASS();
}

/* A block another allocator made (the Windows C runtime, a system DLL) is not a
 * block the layer missed: it is counted apart, so the soundness gate that reads
 * untracked frees sees only real misses (the Windows daemon's 14 CRT blocks
 * tripped it, 2026-09-17). */
TEST(memev_foreign_free_is_counted_apart_from_untracked) {
    fresh();
    cbm_memev_free_foreign(BLK(78));
    cbm_memev_free_foreign(NULL);
    cbm_memev_totals_t t;
    cbm_memev_totals(&t);
    ASSERT_EQ(t.foreign_frees, 1);
    ASSERT_EQ(t.untracked_frees, 0);
    ASSERT_EQ(t.frees, 0);
    PASS();
}

typedef struct {
    int first;
    int count;
    void *site;
} span_t;

static void *alloc_span(void *arg) {
    const span_t *s = (const span_t *)arg;
    for (int i = 0; i < s->count; i++) {
        cbm_memev_alloc(BLK(s->first + i), 48, 64, s->site);
    }
    cbm_memev_flush_thread();
    return NULL;
}

static void *free_span(void *arg) {
    const span_t *s = (const span_t *)arg;
    for (int i = 0; i < s->count; i++) {
        cbm_memev_free(BLK(s->first + i));
    }
    cbm_memev_flush_thread();
    return NULL;
}

enum { MEMEV_T = 4, MEMEV_PER_T = 50000 };

/* Blocks are routinely freed by a thread that did not allocate them, and the
 * per-site totals are SUMS — so they cannot depend on which thread did what or
 * in which order the threads ran. 200,000 live blocks also walks every shard
 * through several growth steps. */
TEST(memev_cross_thread_frees_balance_and_totals_are_order_independent) {
    fresh();
    cbm_thread_t th[MEMEV_T];
    span_t spans[MEMEV_T];
    for (int i = 0; i < MEMEV_T; i++) {
        spans[i] = (span_t){
            .first = i * MEMEV_PER_T, .count = MEMEV_PER_T, .site = (i % 2) ? SITE_A : SITE_B};
        ASSERT_EQ(cbm_thread_create(&th[i], 0, alloc_span, &spans[i]), 0);
    }
    for (int i = 0; i < MEMEV_T; i++) {
        cbm_thread_join(&th[i]);
    }
    cbm_memev_totals_t t;
    cbm_memev_totals(&t);
    ASSERT_EQ(t.allocs, (uint64_t)MEMEV_T * MEMEV_PER_T);
    ASSERT_EQ(t.live_blocks, (uint64_t)MEMEV_T * MEMEV_PER_T);
    ASSERT_EQ(t.live_bytes, (uint64_t)MEMEV_T * MEMEV_PER_T * 64);
    ASSERT_EQ(t.pointer_table_full, 0);

    /* every span is freed by a DIFFERENT thread than the one that allocated it */
    span_t rotated[MEMEV_T];
    for (int i = 0; i < MEMEV_T; i++) {
        rotated[i] = spans[(i + 1) % MEMEV_T];
        ASSERT_EQ(cbm_thread_create(&th[i], 0, free_span, &rotated[i]), 0);
    }
    for (int i = 0; i < MEMEV_T; i++) {
        cbm_thread_join(&th[i]);
    }
    cbm_memev_totals(&t);
    ASSERT_EQ(t.frees, (uint64_t)MEMEV_T * MEMEV_PER_T);
    ASSERT_EQ(t.live_blocks, 0);
    ASSERT_EQ(t.live_bytes, 0);
    ASSERT_EQ(t.untracked_frees, 0);
    cbm_memev_site_t a;
    cbm_memev_site_t b;
    ASSERT_TRUE(site_row(SITE_A, &a));
    ASSERT_TRUE(site_row(SITE_B, &b));
    ASSERT_EQ(a.allocs, (uint64_t)(MEMEV_T / 2) * MEMEV_PER_T);
    ASSERT_EQ(b.usable_bytes, (uint64_t)(MEMEV_T / 2) * MEMEV_PER_T * 64);
    ASSERT_EQ(a.short_lived, 0); /* freed by another thread: never churn */
    PASS();
}

/* Dormant means dormant: with the layer off nothing is recorded and a hint
 * never lingers to mis-attribute a later event. */
TEST(memev_dormant_layer_records_nothing) {
    fresh();
    cbm_memev_force_for_tests(false);
    cbm_memev_hint(SITE_A, 3);
    cbm_memev_alloc(BLK(1), 10, 16, SITE_A);
    ASSERT_FALSE(cbm_memev_hint_pending());
    cbm_memev_free(BLK(1));
    cbm_memev_force_for_tests(true);
    cbm_memev_totals_t t;
    cbm_memev_totals(&t);
    ASSERT_EQ(t.allocs, 0);
    ASSERT_EQ(t.untracked_frees, 0);
    ASSERT_EQ(t.sites, 0);
    PASS();
}

/* The report is JSON lines: one header, then one line per site and one per
 * work row. Ordering is the report tool's job; the dump only has to be
 * complete. */
TEST(memev_dump_writes_header_sites_and_work) {
    fresh();
    cbm_memev_alloc(BLK(1), 10, 16, SITE_A);
    cbm_memev_alloc(BLK(2), 900, 1024, SITE_B);
    cbm_work_note(CBM_WORK_MEMCPY, SITE_C, 4096, 0, 0);
    char path[512];
    snprintf(path, sizeof(path), "%s/cbm_memev_dump_%d.jsonl", cbm_tmpdir(), (int)getpid());
    (void)remove(path);
    ASSERT_TRUE(cbm_memev_dump(path, "test"));
    FILE *f = cbm_fopen(path, "r");
    ASSERT_NOT_NULL(f);
    char line[4096];
    int n = 0;
    bool header = false;
    bool site_a = false;
    bool site_b = false;
    bool work = false;
    while (fgets(line, sizeof(line), f)) {
        if (n++ == 0) {
            header = strstr(line, "\"memwaste\":2") && strstr(line, "\"why\":\"test\"") &&
                     strstr(line, "\"allocs\":2");
        }
        site_a = site_a || strstr(line, "\"site\":\"0xa000\",\"allocs\":1");
        site_b = site_b ||
                 (strstr(line, "\"site\":\"0xb000\"") && strstr(line, "\"usable_bytes\":1024"));
        work =
            work || (strstr(line, "\"work\":\"memcpy\"") && strstr(line, "\"site\":\"0xc000\"") &&
                     strstr(line, "\"bytes\":4096") && strstr(line, "\"peak\":4096"));
    }
    fclose(f);
    (void)remove(path);
    ASSERT_TRUE(header);
    ASSERT_TRUE(site_a);
    ASSERT_TRUE(site_b);
    ASSERT_TRUE(work);
    ASSERT_EQ(n, 4);
    PASS();
}

static bool work_row(cbm_work_kind_t kind, void *site, cbm_work_row_t *out) {
    static cbm_work_row_t rows[128];
    size_t n = cbm_work_rows(rows, 128);
    for (size_t i = 0; i < n; i++) {
        if (rows[i].kind == kind && rows[i].site == (uintptr_t)site) {
            *out = rows[i];
            return true;
        }
    }
    return false;
}

/* The fill pattern is how the layer sees memory nobody wrote. A 4 KB block
 * that got 100 bytes: 3,996 bytes never written, and of the 1,000 requested
 * 900 lie beyond the last write. A hole in the middle is never-written memory
 * too, but it is not over-requested: the bytes after it were used. */
TEST(memev_fill_scan_measures_never_written_and_over_requested) {
    fresh();
    cbm_memev_scan_for_tests(true, false);
    static uint8_t big[4096];
    static uint8_t holed[128];
    cbm_memev_alloc(big, 1000, sizeof(big), SITE_A);
    ASSERT_EQ(big[4095], 0xA5); /* the layer filled it */
    memset(big, 1, 100);
    cbm_memev_free(big);

    cbm_memev_alloc(holed, 80, sizeof(holed), SITE_B);
    memset(holed, 2, 16);      /* [0, 16) written */
    memset(holed + 64, 3, 16); /* [64, 80) written: [16, 64) is a 48-byte hole */
    cbm_memev_free(holed);

    cbm_memev_site_t a;
    cbm_memev_site_t b;
    ASSERT_TRUE(site_row(SITE_A, &a));
    ASSERT_TRUE(site_row(SITE_B, &b));
    ASSERT_EQ(a.scanned_blocks, 1);
    ASSERT_EQ(a.never_written_bytes, 3996);
    ASSERT_EQ(a.over_requested_bytes, 900);
    ASSERT_EQ(b.never_written_bytes, 48 + 48); /* the hole + the unwritten tail */
    ASSERT_EQ(b.over_requested_bytes, 0);
    PASS();
}

/* A calloc'd block is not filled -- its zeros are the contract. Its zero tail
 * at free is an UPPER bound on untouched memory (a written zero looks the same). */
TEST(memev_calloc_tail_is_scanned_for_zeros) {
    fresh();
    cbm_memev_scan_for_tests(true, false);
    static uint8_t block[256];
    memset(block, 0, sizeof(block)); /* what calloc hands out */
    cbm_memev_alloc_ex(block, 256, sizeof(block), SITE_A, CBM_MEMEV_ZEROED);
    ASSERT_EQ(block[200], 0); /* never filled */
    memset(block, 7, 32);
    cbm_memev_free(block);
    cbm_memev_site_t a;
    ASSERT_TRUE(site_row(SITE_A, &a));
    ASSERT_EQ(a.zero_untouched_bytes, 224);
    ASSERT_EQ(a.never_written_bytes, 0);
    PASS();
}

/* A phase boundary is a snapshot: what each site still holds, and which small
 * live blocks carry the same bytes as another -- memory an interning table or
 * a shared constant would have served once. */
TEST(memev_phase_snapshot_finds_retained_memory_and_duplicates) {
    fresh();
    cbm_memev_scan_for_tests(true, true);
    static uint8_t blocks[4][64];
    for (int i = 0; i < 3; i++) {
        cbm_memev_alloc(blocks[i], 16, sizeof(blocks[i]), SITE_A);
        memcpy(blocks[i], "same bytes here!", 16);
    }
    cbm_memev_alloc(blocks[3], 16, sizeof(blocks[3]), SITE_B);
    memcpy(blocks[3], "different bytes!", 16);
    cbm_memev_phase("snapshot");
    cbm_memev_site_t a;
    cbm_memev_site_t b;
    ASSERT_TRUE(site_row(SITE_A, &a));
    ASSERT_TRUE(site_row(SITE_B, &b));
    ASSERT_EQ(a.retained_bytes, 3 * 64);
    ASSERT_EQ(a.dup_blocks, 2); /* the first copy is the original */
    ASSERT_EQ(a.dup_bytes, 2 * 64);
    ASSERT_EQ(b.retained_bytes, 64);
    ASSERT_EQ(b.dup_blocks, 0);
    for (int i = 0; i < 4; i++) {
        cbm_memev_free(blocks[i]);
    }
    PASS();
}

/* Work rows sum per (kind, site); `peak` keeps the largest single note. For a
 * container that is the biggest capacity one instance left unused -- the sums
 * run over USES, the peak is the memory figure. */
TEST(memev_work_rows_sum_and_keep_the_peak) {
    fresh();
    cbm_work_note(CBM_WORK_MEMCPY, SITE_A, 100, 0, 0);
    cbm_work_note(CBM_WORK_MEMCPY, SITE_A, 5000, 0, 0);
    cbm_memev_container(CBM_WORK_CT_ARENA, SITE_B, 1000, 100, 2);
    cbm_memev_container(CBM_WORK_CT_ARENA, SITE_B, 500, 450, 0);
    cbm_work_row_t w;
    ASSERT_TRUE(work_row(CBM_WORK_MEMCPY, SITE_A, &w));
    ASSERT_EQ(w.calls, 2);
    ASSERT_EQ(w.bytes, 5100);
    ASSERT_EQ(w.peak, 5000);
    ASSERT_TRUE(work_row(CBM_WORK_CT_ARENA, SITE_B, &w));
    ASSERT_EQ(w.calls, 2);
    ASSERT_EQ(w.bytes, 1500);
    ASSERT_EQ(w.aux1, 550);
    ASSERT_EQ(w.aux2, 2);
    ASSERT_EQ(w.peak, 900);
    PASS();
}

/* A repeat is work whose answer could not have changed. strlen of the same
 * buffer holding a DIFFERENT string of the same length is new work; a lookup
 * of the same key text repeats even from another buffer, unless the table was
 * written in between. */
TEST(memev_repeats_require_unchanged_input) {
    fresh();
    char buf[16];
    memcpy(buf, "alpha", 6);
    cbm_work_note_strlen(SITE_A, buf, 5);
    cbm_work_note_strlen(SITE_A, buf, 5); /* repeat */
    memcpy(buf, "omega", 6);
    cbm_work_note_strlen(SITE_A, buf, 5); /* same pointer, same length, new text */
    cbm_work_row_t w;
    ASSERT_TRUE(work_row(CBM_WORK_STRLEN, SITE_A, &w));
    ASSERT_EQ(w.calls, 3);
    ASSERT_EQ(w.repeats, 1);

    static const int table = 0;
    char k1[8];
    char k2[8];
    memcpy(k1, "key", 4);
    memcpy(k2, "key", 4);
    cbm_work_note_ht(CBM_WORK_HT_GET, SITE_B, &table, k1, 0, true);
    cbm_work_note_ht(CBM_WORK_HT_GET, SITE_B, &table, k2, 0, true); /* same text: repeat */
    cbm_work_note_ht(CBM_WORK_HT_GET, SITE_B, &table, k2, 1, true); /* table written since */
    cbm_work_note_ht(CBM_WORK_HT_GET, SITE_B, &table, "other", 1, false);
    ASSERT_TRUE(work_row(CBM_WORK_HT_GET, SITE_B, &w));
    ASSERT_EQ(w.calls, 4);
    ASSERT_EQ(w.repeats, 1);
    ASSERT_EQ(w.aux1, 1); /* one miss */

    cbm_work_note_path(CBM_WORK_STAT, SITE_A, "/tmp/cbm-memev-path", false);
    cbm_work_note_path(CBM_WORK_STAT, SITE_C, "/tmp/cbm-memev-path", false); /* process-wide */
    cbm_work_note_path(CBM_WORK_OPEN, SITE_C, "/tmp/cbm-memev-path", true);  /* other kind */
    ASSERT_TRUE(work_row(CBM_WORK_STAT, SITE_C, &w));
    ASSERT_EQ(w.repeats, 1);
    ASSERT_TRUE(work_row(CBM_WORK_OPEN, SITE_C, &w));
    ASSERT_EQ(w.repeats, 0);
    ASSERT_EQ(w.aux1, 1); /* a failure */
    PASS();
}

/* Pools compare their workers by the events each produced. */
TEST(memev_thread_ops_count_this_threads_events) {
    fresh();
    uint64_t before = cbm_memev_thread_ops();
    cbm_memev_alloc(BLK(1), 8, 8, SITE_A);
    cbm_memev_alloc(BLK(2), 8, 8, SITE_A);
    cbm_work_note(CBM_WORK_MEMSET, SITE_B, 8, 0, 0);
    ASSERT_EQ(cbm_memev_thread_ops() - before, 3);
    cbm_memev_free(BLK(1));
    cbm_memev_free(BLK(2));
    PASS();
}

SUITE(mem_events) {
    RUN_TEST(memev_slack_is_charged_to_the_asking_site);
    RUN_TEST(memev_short_lived_blocks_are_counted_as_churn);
    RUN_TEST(memev_realloc_copy_bytes_go_to_the_original_site);
    RUN_TEST(memev_hint_gives_one_event_with_the_cores_site_and_class);
    RUN_TEST(memev_untracked_free_is_counted_not_dropped);
    RUN_TEST(memev_foreign_free_is_counted_apart_from_untracked);
    RUN_TEST(memev_cross_thread_frees_balance_and_totals_are_order_independent);
    RUN_TEST(memev_dormant_layer_records_nothing);
    RUN_TEST(memev_dump_writes_header_sites_and_work);
    RUN_TEST(memev_fill_scan_measures_never_written_and_over_requested);
    RUN_TEST(memev_calloc_tail_is_scanned_for_zeros);
    RUN_TEST(memev_phase_snapshot_finds_retained_memory_and_duplicates);
    RUN_TEST(memev_work_rows_sum_and_keep_the_peak);
    RUN_TEST(memev_repeats_require_unchanged_input);
    RUN_TEST(memev_thread_ops_count_this_threads_events);
}
