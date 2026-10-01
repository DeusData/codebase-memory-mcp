/* The arena lives in src/foundation/arena.h. This file used to be a stale
 * copy with the same include guard and a shorter API; whichever header a
 * translation unit included first won, silently. One definition now. */
#ifndef CBM_INTERNAL_ARENA_SHIM_H
#define CBM_INTERNAL_ARENA_SHIM_H
#include "../../src/foundation/arena.h" /* relative: the lsp_all unit has no -Isrc */
#endif                                  /* CBM_INTERNAL_ARENA_SHIM_H */
