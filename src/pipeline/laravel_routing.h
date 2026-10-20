#ifndef CBM_LARAVEL_ROUTING_H
#define CBM_LARAVEL_ROUTING_H

/*
 * laravel_routing.h — Laravel 11+ framework route mounts (#1146).
 *
 * Laravel 11+ registers route files in bootstrap/app.php:
 *
 *     Application::configure(basePath: dirname(__DIR__))
 *         ->withRouting(web: __DIR__.'/../routes/web.php',
 *                       api: __DIR__.'/../routes/api.php',
 *                       apiPrefix: 'api')
 *
 * and mounts every file passed as `api:` under `apiPrefix` (default 'api');
 * the `web:` file gets no prefix. The prefix is declared in a DIFFERENT file
 * than the routes, so per-file extraction cannot compose it the way it does
 * the in-file `Route::prefix()->group()` chain (#952). The route passes ask
 * here once per PHP file that registers routes and prepend the mount to every
 * Route path minted from that file.
 *
 * Only what bootstrap/app.php states as source fact is used: the nearest
 * ancestor bootstrap/app.php must name the file under `api:`, and a given
 * `apiPrefix:` must be a string literal. A non-literal prefix, positional
 * arguments or a `using:` callback leave the paths untouched — nothing is
 * inferred from file names.
 */

#include "cbm.h"
#include <stdbool.h>
#include <stddef.h>

/* Write the mount ("/api", "/v1/api") under which the Laravel app that owns
 * rel_path serves its routes into out and return true. Returns false with
 * out = "" when rel_path is not mounted under a known, non-empty prefix. */
bool cbm_laravel_api_mount(const char *repo_path, const char *rel_path, char *out, size_t out_sz);

/* Per-file entry for the route passes: probes cbm_laravel_api_mount only for
 * a PHP file that registers at least one path-shaped route; out = "" for
 * every other file, so no other language pays a lookup. */
void cbm_laravel_file_route_mount(const char *repo_path, const char *rel_path, CBMLanguage lang,
                                  const CBMFileResult *result, char *out, size_t out_sz);

/* Compose mount + route path: "/api" + "/users/me" -> "/api/users/me",
 * "/api" + "/" -> "/api". Returns path unchanged when mount is empty or the
 * composed path does not fit buf. */
const char *cbm_laravel_mount_route(const char *mount, const char *path, char *buf, size_t buf_sz);

#endif /* CBM_LARAVEL_ROUTING_H */
