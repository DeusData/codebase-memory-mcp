#ifndef CBM_SHA256_H
#define CBM_SHA256_H

/* In-process SHA-256 (FIPS 180-4). Used to verify the integrity of a
 * downloaded release before installing it, without shelling out to a
 * platform hashing tool (shasum / sha256sum / certutil) — those differ per
 * OS, may be absent, and mis-quote paths under cmd.exe. */

#include <stddef.h>
#include <stdint.h>

#define CBM_SHA256_DIGEST_LEN 32 /* raw digest bytes */
#define CBM_SHA256_HEX_LEN 64    /* lowercase hex chars (no NUL) */

typedef struct {
    uint32_t state[8];
    uint64_t bitlen;
    uint8_t buf[64];
    size_t buflen;
    /* Message-schedule scratch for one compression round. It lives in the
     * context rather than in sha256_transform's own frame because a 256-byte
     * local there is large enough for ASan's use-after-return fake stack,
     * which then heap-allocates it on EVERY 64-byte block — millions of
     * __asan_stack_malloc calls to fingerprint one large file, turning
     * sanitized binary fingerprinting into minutes. Here it is allocated once
     * per hash instead of once per block. */
    uint32_t sched[64];
} cbm_sha256_ctx;

void cbm_sha256_init(cbm_sha256_ctx *c);
void cbm_sha256_update(cbm_sha256_ctx *c, const void *data, size_t len);
void cbm_sha256_final(cbm_sha256_ctx *c, uint8_t out[CBM_SHA256_DIGEST_LEN]);

/* One-shot hash of a buffer to lowercase hex. `out` must hold
 * CBM_SHA256_HEX_LEN + 1 bytes (hex chars + NUL). */
void cbm_sha256_hex(const void *data, size_t len, char out[CBM_SHA256_HEX_LEN + 1]);

/* RFC 2104 HMAC-SHA-256. The output is always CBM_SHA256_DIGEST_LEN bytes.
 * A NULL key/data pointer is accepted only when its corresponding length is
 * zero. */
void cbm_hmac_sha256(const void *key, size_t key_len, const void *data, size_t data_len,
                     uint8_t out[CBM_SHA256_DIGEST_LEN]);

#if defined(CBM_ENABLE_TEST_SEAMS) && CBM_ENABLE_TEST_SEAMS
#include <stdbool.h>
/* The block backend in use: "arm-sha2", "x86-sha-ni" or "portable". */
const char *cbm_sha256_backend_name_for_testing(void);
/* Force the portable transform (true) or return to the probed backend (false),
 * so a test can hash the same input both ways. */
void cbm_sha256_force_portable_for_testing(bool force);

/* TEMPORARY diagnostic (drop before merge): per-hash CPU facts for the hosted
 * x86 runners whose full-image fingerprint is ~40x slower on Intel than on
 * AMD. Enabled once per process (before any thread starts); when enabled, the
 * x86 SHA-NI block path samples XINUSE (XGETBV ECX=1) before each call, per
 * thread, to show whether dirty upper YMM/ZMM state precedes the legacy-SSE
 * SHA-NI code. Reset and take refer to the calling thread's counters. */
void cbm_sha256_diag_enable_for_testing(bool enable);
void cbm_sha256_diag_reset_for_testing(void);
void cbm_sha256_diag_take_for_testing(uint64_t *x86_calls, uint64_t *dirty_upper_calls,
                                      uint64_t *xinuse_or);
/* One "key=value ..." summary of the CPU: vendor/family/model/stepping, the
 * CPUID SHA/AVX/AVX-512F bits, XINUSE readability and the SSE encoding the
 * SHA-NI path was compiled with. */
void cbm_sha256_diag_cpu_for_testing(char *out, size_t cap);
#endif

#endif /* CBM_SHA256_H */
