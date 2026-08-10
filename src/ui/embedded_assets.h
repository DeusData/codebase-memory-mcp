/*
 * embedded_assets.h — Interface for embedded frontend assets.
 *
 * When built with `cbm-with-ui`, the embed script generates embedded_assets.c
 * with actual file data. Otherwise, embedded_stub.c provides an empty array.
 */
#ifndef CBM_UI_EMBEDDED_ASSETS_H
#define CBM_UI_EMBEDDED_ASSETS_H

#include <stddef.h>
#include <string.h>

typedef struct {
    const char *path;          /* URL path, e.g. "/index.html" */
    const unsigned char *data; /* raw file bytes */
    unsigned int size;         /* byte count */
    const char *content_type;  /* MIME type, e.g. "text/html" */
} cbm_embedded_file_t;

/* Array of embedded files + count. Defined in embedded_assets.c or embedded_stub.c.
 * Not const — sizes are initialized at startup by a constructor function. */
extern cbm_embedded_file_t CBM_EMBEDDED_FILES[];
extern const int CBM_EMBEDDED_FILE_COUNT;

/* The generator emits bytewise path order; keep lookup logarithmic as the
 * frontend gains chunks. */
static inline const cbm_embedded_file_t *
cbm_embedded_lookup_sorted(const cbm_embedded_file_t *files, size_t count, const char *path) {
    if (!files || !path) {
        return NULL;
    }
    size_t low = 0;
    size_t high = count;
    while (low < high) {
        size_t middle = low + (high - low) / 2U;
        int compared = strcmp(path, files[middle].path);
        if (compared == 0) {
            return &files[middle];
        }
        if (compared < 0) {
            high = middle;
        } else {
            low = middle + 1U;
        }
    }
    return NULL;
}

/* Look up an embedded file by URL path. Returns NULL if not found. */
const cbm_embedded_file_t *cbm_embedded_lookup(const char *path);

#endif /* CBM_UI_EMBEDDED_ASSETS_H */
