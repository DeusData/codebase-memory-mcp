/* nomic-embed-code (nomic-ai/nomic-embed-code) token embeddings.
 * 40856 tokens x 768d int8-quantized unit vectors.
 * Distilled from 7B model via full inference on filtered vocabulary.
 * Simulated attention: 3 iterations, K=32, alpha=0.3.
 *
 * Vector blob embedded via code_vectors_blob.S (assembler .incbin).
 * Token strings are in this header as a static array.
 *
 * Storage format: int8 × 127. We also tested float32 storage — it did NOT
 * improve performance because cooccur passes are memory-bandwidth-bound.
 * Float32 dense reads are 4x larger than int8, which cancels the CPU savings
 * from avoided int8→float conversion. int8 is a strict win on binary size
 * (30 MB vs 120 MB) and equal on runtime.
 *
 * Source: https://huggingface.co/nomic-ai/nomic-embed-code
 * License: Apache 2.0
 */
#ifndef CBM_NOMIC_VECTORS_H
#define CBM_NOMIC_VECTORS_H

#include <stdint.h>

#define PRETRAINED_TOKEN_COUNT 40856
#define PRETRAINED_DIM 768

/* Raw vector blob: first 8 bytes = [int32 count][int32 dim],
 * then count x dim int8 values (unit-normalized, x127 scaled). */
extern const unsigned char PRETRAINED_VECTOR_BLOB[];
extern const unsigned int PRETRAINED_VECTOR_BLOB_LEN;

/* Access the int8 vector for token index i. Zero-copy pointer into blob. */
static inline const int8_t *pretrained_vec_at(int i) {
    return (const int8_t *)(PRETRAINED_VECTOR_BLOB + 8 + (size_t)i * PRETRAINED_DIM);
}

/* Token strings (separate header to keep this file clean). */
#include "code_tokens.h"

#endif /* CBM_NOMIC_VECTORS_H */
