#ifndef IF_NATIVE_GIT_PATH_H
#define IF_NATIVE_GIT_PATH_H

#include <stdbool.h>
#include <stddef.h>

/* Test-only bounded PATH lookup, not a production executable trust decision.
 * out[0] is cleared on every failure when out/capacity permit it.
 * No printing, environment mutation, allocation or process execution.
 * Caller serializes environment mutation and keeps out disjoint from PATH.
 * Empty/relative PATH entries (including '.') never trigger a current-directory
 * search. An explicitly absolute entry is honored, even if it names cwd.
 * The fixture MUST validate the returned candidate with genuine Git commands.
 */
bool if_native_git_path(char *out, size_t capacity);

#endif
