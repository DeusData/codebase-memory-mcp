/*
 * content_hash.h — Stable content fingerprints for incremental indexing.
 */
#ifndef CBM_PIPELINE_CONTENT_HASH_H
#define CBM_PIPELINE_CONTENT_HASH_H

/* "xxh3-128:" + 32 lowercase hexadecimal digits + NUL. */
#define CBM_CONTENT_HASH_SIZE 42

/* Hash a file with streaming XXH3-128.
 * Returns 0 on success and writes a NUL-terminated, algorithm-tagged digest.
 * Returns -1 on invalid input, open/read failure, or allocation failure. */
int cbm_content_hash_file(const char *path, char out[CBM_CONTENT_HASH_SIZE]);

#endif /* CBM_PIPELINE_CONTENT_HASH_H */
