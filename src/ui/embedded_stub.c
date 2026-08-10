/*
 * embedded_stub.c — Empty asset table when built without frontend.
 *
 * Used by the standard `cbm` target (no Node.js required).
 * The `cbm-with-ui` target replaces this with generated embedded_assets.c.
 */
#include "ui/embedded_assets.h"

#include <stddef.h>
#include <string.h>

cbm_embedded_file_t CBM_EMBEDDED_FILES[] = {{NULL, NULL, 0, NULL}};
const int CBM_EMBEDDED_FILE_COUNT = 0;

const cbm_embedded_file_t *cbm_embedded_lookup(const char *path) {
    for (int i = 0; i < CBM_EMBEDDED_FILE_COUNT; i++) {
        if (strcmp(CBM_EMBEDDED_FILES[i].path, path) == 0) {
            return &CBM_EMBEDDED_FILES[i];
        }
    }
    return NULL;
}
