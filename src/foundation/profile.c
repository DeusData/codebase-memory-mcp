/*
 * profile.c — Activatable profiling implementation.
 */
#include "foundation/profile.h"
#include "foundation/log.h"
#include "foundation/compat.h"
#include "foundation/platform.h"

#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

enum {
    PROF_BUF_LEN = 32,
    PROF_NS_PER_US = 1000,
    PROF_US_PER_MS = 1000,
    PROF_US_PER_SEC = 1000000,
};
#define PROF_US_PER_SEC_D 1000000.0

bool cbm_profile_active = false;

void cbm_profile_init(void) {
    if (cbm_env_flag_enabled("CBM_PROFILE")) {
        cbm_profile_active = true;
        cbm_log_set_profile_stderr_mirror(true);
    }
}

void cbm_profile_enable(void) {
    cbm_profile_active = true;
    cbm_log_set_profile_stderr_mirror(true);
}

void cbm_profile_now(struct timespec *ts) {
    cbm_clock_gettime(CLOCK_MONOTONIC, ts);
}

void cbm_profile_log_elapsed(const char *phase, const char *sub, const struct timespec *start,
                             long items) {
    struct timespec now;
    cbm_clock_gettime(CLOCK_MONOTONIC, &now);

    long us = ((long)(now.tv_sec - start->tv_sec) * PROF_US_PER_SEC) +
              ((now.tv_nsec - start->tv_nsec) / PROF_NS_PER_US);
    long ms = us / PROF_US_PER_MS;

    char ms_buf[PROF_BUF_LEN];
    char us_buf[PROF_BUF_LEN];
    char items_buf[PROF_BUF_LEN];
    snprintf(ms_buf, sizeof(ms_buf), "%ld", ms);
    snprintf(us_buf, sizeof(us_buf), "%ld", us);

    if (items > 0 && us > 0) {
        long rate = (long)((double)items * PROF_US_PER_SEC_D / (double)us);
        char rate_buf[PROF_BUF_LEN];
        snprintf(items_buf, sizeof(items_buf), "%ld", items);
        snprintf(rate_buf, sizeof(rate_buf), "%ld", rate);
        cbm_log_info("prof", "phase", phase, "sub", sub, "ms", ms_buf, "us", us_buf, "items",
                     items_buf, "rate_per_s", rate_buf);
    } else if (items > 0) {
        snprintf(items_buf, sizeof(items_buf), "%ld", items);
        cbm_log_info("prof", "phase", phase, "sub", sub, "ms", ms_buf, "us", us_buf, "items",
                     items_buf);
    } else {
        cbm_log_info("prof", "phase", phase, "sub", sub, "ms", ms_buf, "us", us_buf);
    }
}

/* ── Scaling probe ──────────────────────────────────────────────────── */

enum {
    /* Below this, scheduling noise swamps the fit and every short pass would
     * cry wolf. A pass with fewer items than this is not where an O(n^2)
     * regression hurts anyone. */
    SCALE_MIN_ITEMS = 512,
    /* Likewise in time: a first checkpoint under a millisecond is measuring the
     * clock, not the workload. */
    SCALE_MIN_FIRST_US = 1000,
};

static long scale_elapsed_us(const struct timespec *start) {
    struct timespec now;
    cbm_clock_gettime(CLOCK_MONOTONIC, &now);
    return ((long)(now.tv_sec - start->tv_sec) * PROF_US_PER_SEC) +
           ((now.tv_nsec - start->tv_nsec) / PROF_NS_PER_US);
}

void cbm_scale_begin(cbm_scale_probe_t *probe, const char *phase, long total) {
    if (!probe) {
        return;
    }
    probe->phase = phase;
    probe->total = total;
    atomic_init(&probe->next_cp, 0);
    for (int i = 0; i < CBM_SCALE_CHECKPOINTS; i++) {
        probe->cp_us[i] = 0;
        probe->cp_items[i] = 0;
    }
    cbm_clock_gettime(CLOCK_MONOTONIC, &probe->start);
}

void cbm_scale_tick(cbm_scale_probe_t *probe, long done) {
    if (!probe || probe->total < SCALE_MIN_ITEMS) {
        return;
    }
    int cp = atomic_load_explicit(&probe->next_cp, memory_order_relaxed);
    if (cp >= CBM_SCALE_CHECKPOINTS) {
        return;
    }
    /* cp 0..3 -> total/8, total/4, total/2, total */
    long threshold = probe->total >> (CBM_SCALE_CHECKPOINTS - 1 - cp);
    if (done < threshold) {
        return;
    }
    /* Exactly one thread records each checkpoint; a loser simply moves on and
     * will re-evaluate against the next threshold on its following tick. */
    if (!atomic_compare_exchange_strong_explicit(&probe->next_cp, &cp, cp + 1, memory_order_relaxed,
                                                 memory_order_relaxed)) {
        return;
    }
    probe->cp_us[cp] = scale_elapsed_us(&probe->start);
    probe->cp_items[cp] = done;
}

double cbm_scale_fit_k(long first_n, long first_us, long last_n, long last_us) {
    if (first_n <= 0 || first_us <= 0 || last_us <= 0 || last_n <= first_n) {
        return -1.0;
    }
    /* k = log(T_last / T_first) / log(n_last / n_first) */
    return log((double)last_us / (double)first_us) / log((double)last_n / (double)first_n);
}

void cbm_scale_end(cbm_scale_probe_t *probe) {
    if (!probe || probe->total < SCALE_MIN_ITEMS) {
        return;
    }
    int recorded = atomic_load_explicit(&probe->next_cp, memory_order_relaxed);
    if (recorded < 2) {
        return; /* need two points to speak about growth at all */
    }
    long first_us = probe->cp_us[0];
    long first_n = probe->cp_items[0];
    long last_us = probe->cp_us[recorded - 1];
    long last_n = probe->cp_items[recorded - 1];
    if (first_us < SCALE_MIN_FIRST_US || first_n <= 0 || last_n <= first_n || last_us <= 0) {
        return;
    }

    double k = cbm_scale_fit_k(first_n, first_us, last_n, last_us);
    if (k < 0.0) {
        return;
    }
    long us_per_item = last_us / last_n;

    char k_buf[PROF_BUF_LEN];
    char n_buf[PROF_BUF_LEN];
    char per_buf[PROF_BUF_LEN];
    char ms_buf[PROF_BUF_LEN];
    snprintf(k_buf, sizeof(k_buf), "%.2f", k);
    snprintf(n_buf, sizeof(n_buf), "%ld", last_n);
    snprintf(per_buf, sizeof(per_buf), "%ld", us_per_item);
    snprintf(ms_buf, sizeof(ms_buf), "%ld", last_us / PROF_US_PER_MS);

    if (k >= CBM_SCALE_WARN_K) {
        /* Unflagged on purpose: this is the alarm the next accidental O(n^2)
         * should trip in an ordinary user's log. */
        cbm_log_warn("scaling.superlinear", "phase", probe->phase, "k", k_buf, "items", n_buf,
                     "elapsed_ms", ms_buf, "us_per_item", per_buf);
    }
    if (!cbm_profile_active) {
        return;
    }
    char curve[PROF_BUF_LEN * CBM_SCALE_CHECKPOINTS];
    int used = 0;
    for (int i = 0; i < recorded && used < (int)sizeof(curve) - 1; i++) {
        int wrote =
            snprintf(curve + used, sizeof(curve) - (size_t)used, "%s%ld:%ld", i == 0 ? "" : ",",
                     probe->cp_items[i], probe->cp_us[i] / PROF_US_PER_MS);
        if (wrote < 0) {
            break;
        }
        used += wrote;
    }
    cbm_log_info("scaling", "phase", probe->phase, "k", k_buf, "items", n_buf, "elapsed_ms", ms_buf,
                 "us_per_item", per_buf, "curve_items_ms", curve);
}
