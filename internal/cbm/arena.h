/*
 * Extraction compatibility include.
 *
 * CBMArena used to be duplicated here and in src/foundation/arena.h. The
 * production binary links the foundation implementation, so any field drift
 * between those definitions is an ABI violation. Keep one canonical layout.
 */
#ifndef CBM_EXTRACTION_ARENA_COMPAT_H
#define CBM_EXTRACTION_ARENA_COMPAT_H

#include "../../src/foundation/arena.h"

#endif /* CBM_EXTRACTION_ARENA_COMPAT_H */
