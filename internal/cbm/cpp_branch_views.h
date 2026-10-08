#ifndef CBM_CPP_BRANCH_VIEWS_H
#define CBM_CPP_BRANCH_VIEWS_H

#include "cbm.h"

/*
 * Branch views of a source whose grammar parses only the first branch of an
 * #if/#else group (the Haskell scanner swallows everything from #else to
 * #endif as one token, so a definition written once per configuration is seen
 * in its first variant only).
 *
 * A view keeps the source byte for byte and line for line: the branch it
 * selects in every group stays, the other branches and all conditional lines
 * (#if/#ifdef/#ifndef/#else/#elif.../#endif) become spaces, newlines stay. So
 * every span extracted from a view is a raw-source span.
 *
 * View v (1-based) selects branch min(v, branches - 1) of a group with at
 * least two branches, and the only branch of a group without one.
 */

#define CBM_CPP_BRANCH_VIEWS_MAX 3

// True when `lang`'s grammar parses only the first branch of a conditional.
bool cbm_lang_needs_cpp_branch_views(CBMLanguage lang);

// Number of views the source needs (0 when no group has a second branch),
// at most CBM_CPP_BRANCH_VIEWS_MAX.
int cbm_cpp_branch_view_count(CBMArena *a, const char *src, int len);

// View `view` (1..count) as an arena buffer of exactly `len` bytes plus a NUL,
// or NULL on allocation failure.
char *cbm_cpp_branch_view(CBMArena *a, const char *src, int len, int view);

#endif
