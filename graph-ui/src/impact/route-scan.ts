/**
 * The routes of a project, read off its source text because the index has none.
 *
 * The gap is the reference project's and it is unchanged here: the analysis
 * writes route nodes for annotation-driven Java and reports them under the
 * `routes` aspect, and writes none at all for TypeScript, JavaScript and Python.
 * Measured against the built server of PR 1860 on fixtures/atlas-sample,
 * `get_architecture` carries no `routes` key whatsoever. A change view that read
 * only the index would therefore tell a Node reader that their application has
 * no endpoints, which is the confident-empty-list failure this product exists to
 * avoid.
 *
 * src/provider/route-reader.ts is the pure reader and it was ported unchanged;
 * what it needs and does not have in a browser is somebody to hand it the text.
 * That is this file. The provider's own `scanRoutes` cannot do it: it wants a
 * synchronous file system, and here every file is a request.
 *
 * Three bounds, and all three are reported rather than applied quietly:
 *
 *  1. **A cap on files.** Every file is two requests (the module node, then the
 *     snippet), so a repository of two thousand sources would be four thousand
 *     round trips for one panel. {@link MAX_ROUTE_SCAN_FILES} is where the scan
 *     stops, and {@link RouteScan.cappedAt} says so.
 *  2. **Only the extensions the reader knows.** Anything else is skipped whole
 *     and not counted as scanned, because "we looked and found nothing" and "we
 *     did not look" are different claims.
 *  3. **A file that arrived incomplete is named.** The reader gets whatever the
 *     snippet route returned, and a registration past the cut is not on the
 *     list. {@link RouteScan.truncatedFiles} is how a surface says so.
 *
 * Every route recovered this way is marked `source` all the way to the screen,
 * exactly as in the reference.
 */

import { isRouteSource, readRoutes } from '../provider/route-reader';
import type { RouteRef } from '../core/intelligence-provider';

/** How many source files one scan opens. A bound on cost, not on correctness. */
export const MAX_ROUTE_SCAN_FILES = 40;

/** What one file's text was, and whether all of it arrived. */
export interface ScannedSource {
    source: string;
    truncated: boolean;
}

/** What a scan found and what it did not look at. */
export interface RouteScan {
    routes: RouteRef[];
    /** Files whose text was actually read. */
    scanned: number;
    /** The cap, when it was reached. Absent when every candidate was opened. */
    cappedAt?: number;
    /** Files whose text arrived incomplete, so a registration past the cut is missing. */
    truncatedFiles: number;
}

/**
 * Read the route registrations of a set of indexed files.
 *
 * The files are taken in the order the index listed them, which is sorted, so
 * two runs against one index scan the same files and the cap always cuts the
 * same tail. A file that cannot be read is skipped in silence and not counted:
 * the index listed it, so its absence is a race with somebody's editor and not a
 * finding about the repository.
 */
export async function scanRoutes(
    files: readonly string[],
    read: (filePath: string) => Promise<ScannedSource | undefined>,
    cap = MAX_ROUTE_SCAN_FILES,
): Promise<RouteScan> {
    const candidates = files.filter(isRouteSource);
    const opened = candidates.slice(0, cap);
    const routes: RouteRef[] = [];
    let scanned = 0;
    let truncatedFiles = 0;
    for (const filePath of opened) {
        const text = await read(filePath).catch(() => undefined);
        if (text === undefined) {
            continue;
        }
        scanned += 1;
        if (text.truncated) {
            truncatedFiles += 1;
        }
        for (const route of readRoutes(text.source, filePath)) {
            routes.push({ ...route, filePath, origin: 'source' });
        }
    }
    return {
        routes,
        scanned,
        ...(candidates.length > opened.length ? { cappedAt: cap } : {}),
        truncatedFiles,
    };
}

/**
 * The summary's own routes and the scanned ones as one list.
 *
 * Merged rather than chosen between, exactly as the provider merges them: a Java
 * project gets paths from the index that carry no file, and a TypeScript project
 * gets nothing at all from the index. Deduplication is on method and path, and
 * the scan wins a tie because it is the reading that knows where the
 * registration is written, which is what a reader clicks on.
 */
export function mergeRoutes(scanned: readonly RouteRef[], fromIndex: readonly RouteRef[]): RouteRef[] {
    const seen = new Set(scanned.map((route) => `${route.method ?? ''} ${route.path}`));
    const out = [...scanned];
    for (const route of fromIndex) {
        const key = `${route.method ?? ''} ${route.path}`;
        if (route.path.length === 0 || seen.has(key)) {
            continue;
        }
        seen.add(key);
        out.push(route);
    }
    return out;
}
