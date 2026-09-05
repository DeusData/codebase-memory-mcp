/**
 * What the entry-point dialog offers, as a plain list.
 *
 * Two sources feed it and they are not the same claim, so the list says which
 * each row came from. The summary's entry points are symbols the analysis
 * flagged as ways into the program. A route is a registration the product
 * recovered, and `origin` on it separates the two ways that can happen: `index`
 * means the analysis reported the route, `source` means CodeAtlas read it out of
 * the file text, which is the weaker reading and is labelled as such. The
 * reference draws the same distinction on the same field
 * (`RouteRef.origin` in core/common/intelligence-provider.ts), and a dialog that
 * showed both identically would be overstating the second.
 *
 * A row that carries no file is still shown and is not selectable: the index
 * named it, so hiding it would report fewer ways in than were found, and opening
 * it would mean inventing a path.
 */

import type { ArchitectureOverviewDto, RouteRef, SymbolSearchHit } from '../core/intelligence-provider';
import type { DeclarationTarget } from '../twin/twin-target';

/**
 * How many ways in the dialog lists.
 *
 * Forty is past what anybody scans and short of what makes the overlay a page of
 * its own. Past it the list says how many it is not showing, and the search field
 * is the way to the rest: a truncated list that pretended to be complete would be
 * the one place this dialog could quietly hide the entry point somebody wanted.
 */
export const MAX_ENTRY_ROWS = 40;

/** One offered way in. */
export interface EntryRow {
    /** Stable within one list: what identifies the row. */
    key: string;
    name: string;
    /** Workspace-relative path with the declaration line, or empty. */
    where: string;
    /** Where this row came from, in the reader's words. */
    origin: string;
    /** What to open, or undefined when the index named no file. */
    target?: DeclarationTarget;
}

/** The headline over the list: what it holds and what it is not showing. */
export function entryHeadline(total: number, shown: number): string {
    if (total === 0) {
        return 'the index flagged no entry point for this project';
    }
    if (shown < total) {
        return `${total} ways in the index flagged, ${shown} shown: search for the rest`;
    }
    return total === 1 ? '1 way in the index flagged' : `${total} ways in the index flagged`;
}

/**
 * Why there is no route in the list, when there is none.
 *
 * An absence is not a finding, and a dialog that offered "17 ways in" with no
 * route among them would be quietly claiming this project exposes none. Two
 * things are true here and both are said: the analysis reports no route for
 * every language it does not recover routes for, and this frontend has no file
 * system, so the source scan that fills that gap in the reference project
 * cannot run at all. Empty when the list does hold routes, because then there
 * is nothing to explain.
 */
export const NO_ROUTES_NOTE =
    'no route is listed: the index reported none for this project, and this surface reads no source '
    + 'files of its own, so an unreported route cannot be recovered here';

/** The sentence under the list, or empty when the list needs no sentence. */
export function routeNote(
    overview: Pick<ArchitectureOverviewDto, 'entryPoints' | 'routes'> | undefined,
): string {
    if (overview === undefined || (overview.routes ?? []).length > 0) {
        return '';
    }
    return NO_ROUTES_NOTE;
}

function whereOf(filePath: string | undefined, line: number | undefined): string {
    if (filePath === undefined || filePath.length === 0) {
        return '';
    }
    return line === undefined ? filePath : `${filePath}:${line}`;
}

/** One flagged entry point as a row. */
function rowOfHit(hit: SymbolSearchHit): EntryRow {
    return {
        key: `symbol:${hit.qualifiedName ?? `${hit.filePath ?? ''}#${hit.name}`}`,
        name: hit.name,
        where: whereOf(hit.filePath, hit.line),
        origin: 'entry point',
        ...(hit.filePath === undefined || hit.filePath.length === 0
            ? {}
            : {
                target: {
                    name: hit.name,
                    qualifiedName: hit.qualifiedName,
                    kind: hit.kind,
                    filePath: hit.filePath,
                    startLine: hit.line,
                } satisfies DeclarationTarget,
            }),
    };
}

/** One recovered route as a row, with the strength of the reading on it. */
function rowOfRoute(route: RouteRef): EntryRow {
    const label = `${route.method === undefined ? '' : `${route.method} `}${route.path}`;
    return {
        key: `route:${label}`,
        name: route.handler === undefined || route.handler.length === 0 ? label : `${label} -> ${route.handler}`,
        where: whereOf(route.filePath, route.line),
        origin: route.origin === 'index' ? 'route (from the index)' : 'route (read from the source)',
        ...(route.filePath === undefined || route.filePath.length === 0
            ? {}
            : {
                target: {
                    name: route.handler === undefined || route.handler.length === 0 ? label : route.handler,
                    kind: 'route',
                    filePath: route.filePath,
                    startLine: route.line,
                } satisfies DeclarationTarget,
            }),
    };
}

/**
 * The offered rows, entry points first and routes after them.
 *
 * Entry points first because they are the stronger claim; both groups keep the
 * order the summary gave, which the provider already sorted, so the list does
 * not reshuffle between two openings of the same dialog.
 */
export function entryRows(
    overview: Pick<ArchitectureOverviewDto, 'entryPoints' | 'routes'> | undefined,
    limit: number = MAX_ENTRY_ROWS,
): { rows: EntryRow[]; total: number } {
    const all = [
        ...(overview?.entryPoints ?? []).map(rowOfHit),
        ...(overview?.routes ?? []).map(rowOfRoute),
    ];
    const seen = new Set<string>();
    const unique = all.filter((row) => {
        if (seen.has(row.key)) {
            return false;
        }
        seen.add(row.key);
        return true;
    });
    return { rows: unique.slice(0, Math.max(0, limit)), total: unique.length };
}
