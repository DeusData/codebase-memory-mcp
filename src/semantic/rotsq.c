/*
 * rotsq.c — see rotsq.h. From-paper implementation, no dependencies.
 */
#include "semantic/rotsq.h"

#include <stdatomic.h>
#include <string.h>

#define XXH_INLINE_ALL
#include "xxhash/xxhash.h"

/* ── Deterministic ±1 diagonal (seeded, generated once) ─────────────── */

static float g_rsq_diag[CBM_RSQ_DIM];
enum {
    RSQ_DIAG_UNINITIALIZED = 0,
    RSQ_DIAG_INITIALIZING,
    RSQ_DIAG_READY,
};
static atomic_int g_rsq_diag_state = ATOMIC_VAR_INIT(RSQ_DIAG_UNINITIALIZED);

static void rsq_init_diag(void) {
    if (atomic_load_explicit(&g_rsq_diag_state, memory_order_acquire) == RSQ_DIAG_READY) {
        return;
    }

    int expected = RSQ_DIAG_UNINITIALIZED;
    if (atomic_compare_exchange_strong_explicit(&g_rsq_diag_state, &expected, RSQ_DIAG_INITIALIZING,
                                                memory_order_acq_rel, memory_order_acquire)) {
        for (int d = 0; d < CBM_RSQ_DIM; d++) {
            uint64_t h = XXH3_64bits_withSeed(&d, sizeof(d), 0x5bd1e995u);
            g_rsq_diag[d] = (h & 1u) ? 1.0F : -1.0F;
        }
        atomic_store_explicit(&g_rsq_diag_state, RSQ_DIAG_READY, memory_order_release);
        return;
    }

    /* Initialization is bounded and cannot fail.  Acquire pairs with the
     * initializer's release so readers observe the fully populated table. */
    while (atomic_load_explicit(&g_rsq_diag_state, memory_order_acquire) != RSQ_DIAG_READY) {}
}

/* ── Fast Walsh–Hadamard Transform (in place, unnormalized) ─────────── */

static void rsq_fwht(float *v) {
    for (int len = 1; len < CBM_RSQ_DIM; len <<= 1) {
        for (int i = 0; i < CBM_RSQ_DIM; i += len << 1) {
            for (int j = i; j < i + len; j++) {
                float a = v[j];
                float b = v[j + len];
                v[j] = a + b;
                v[j + len] = a - b;
            }
        }
    }
}

/* ── Encode ─────────────────────────────────────────────────────────── */

void cbm_rsq_encode(const float *v, cbm_rsq_code_t *out) {
    rsq_init_diag();

    float rot[CBM_RSQ_DIM];
    for (int d = 0; d < CBM_RSQ_IN_DIM; d++) {
        rot[d] = v[d] * g_rsq_diag[d];
    }
    for (int d = CBM_RSQ_IN_DIM; d < CBM_RSQ_DIM; d++) {
        rot[d] = 0.0F;
    }
    rsq_fwht(rot);
    /* Normalize the transform so the rotation is orthonormal (H/√D): keeps
     * the coordinates in a data-independent range and makes the estimated IP
     * directly comparable to the pre-rotation IP. */
    const float inv_sqrt_d = 1.0F / 32.0F; /* 1/√1024 */
    float lo = rot[0] * inv_sqrt_d;
    float hi = lo;
    for (int d = 0; d < CBM_RSQ_DIM; d++) {
        rot[d] *= inv_sqrt_d;
        if (rot[d] < lo) {
            lo = rot[d];
        }
        if (rot[d] > hi) {
            hi = rot[d];
        }
    }

    /* Per-vector scalar quantization over [lo, hi] at CBM_RSQ_BITS. */
    float range = hi - lo;
    float step = range > 0.0F ? range / (float)CBM_RSQ_LEVELS : 1.0F;
    out->offset = lo;
    out->scale = step;

    int32_t sum = 0;
    memset(out->codes, 0, sizeof(out->codes));
    for (int d = 0; d < CBM_RSQ_DIM; d++) {
        float q = (rot[d] - lo) / step;
        int32_t c = (int32_t)(q + 0.5F);
        if (c < 0) {
            c = 0;
        }
        if (c > CBM_RSQ_LEVELS) {
            c = CBM_RSQ_LEVELS;
        }
        sum += c;
        if (d & 1) {
            out->codes[d >> 1] |= (uint8_t)(c << 4);
        } else {
            out->codes[d >> 1] |= (uint8_t)c;
        }
    }
    out->code_sum = sum;
}

/* ── Estimated inner product from two codes ─────────────────────────── */

float cbm_rsq_ip(const cbm_rsq_code_t *a, const cbm_rsq_code_t *b) {
    /* Integer dot of the 4-bit codes. */
    int64_t dot = 0;
    for (int i = 0; i < CBM_RSQ_CODE_BYTES; i++) {
        uint8_t ba = a->codes[i];
        uint8_t bb = b->codes[i];
        dot += (int64_t)(ba & 0x0F) * (bb & 0x0F);
        dot += (int64_t)(ba >> 4) * (bb >> 4);
    }
    /* x_i ≈ oa + sa·ca_i, y_i ≈ ob + sb·cb_i (in the rotated space, where the
     * IP equals the original IP because the rotation is orthonormal). */
    double d = (double)CBM_RSQ_DIM;
    double ip = d * (double)a->offset * (double)b->offset +
                (double)a->offset * (double)b->scale * (double)b->code_sum +
                (double)b->offset * (double)a->scale * (double)a->code_sum +
                (double)a->scale * (double)b->scale * (double)dot;
    return (float)ip;
}

/* ── Dequantize into the rotated basis ──────────────────────────────── */

void cbm_rsq_decode(const cbm_rsq_code_t *c, float *out) {
    for (int i = 0; i < CBM_RSQ_CODE_BYTES; i++) {
        uint8_t b = c->codes[i];
        out[i * 2] = c->offset + c->scale * (float)(b & 0x0F);
        out[i * 2 + 1] = c->offset + c->scale * (float)(b >> 4);
    }
}
