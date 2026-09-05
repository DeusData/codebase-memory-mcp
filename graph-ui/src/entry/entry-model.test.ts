/**
 * Die Liste der Wege hinein.
 *
 * Zwei Eigenschaften: die Zeile sagt, woher sie kommt, und eine Zeile ohne
 * Datei bleibt stehen, ohne anklickbar zu sein. Beide sind Ehrlichkeitsregeln
 * und keine Anzeigedetails.
 */

import { describe, expect, it } from 'vitest';

import type { RouteRef, SymbolSearchHit } from '../core/intelligence-provider';
import { NO_ROUTES_NOTE, entryHeadline, entryRows, routeNote } from './entry-model';

const hits: SymbolSearchHit[] = [
    { name: 'loadConfig', qualifiedName: 'p.src.config.loadConfig', kind: 'function', filePath: 'src/config.ts', line: 11 },
    { name: 'createUser', qualifiedName: 'p.src.services.userService.createUser', kind: 'function', filePath: 'src/services/userService.ts', line: 23 },
    // Named by the index, placed nowhere: still listed, never openable.
    { name: 'nowhere', qualifiedName: 'p.src.types.nowhere', kind: 'function' },
];

const routes: RouteRef[] = [
    { method: 'GET', path: '/users', filePath: 'src/routes/users.ts', line: 9, handler: 'listUsers', origin: 'source' },
    { method: 'POST', path: '/orders', origin: 'index' },
];

describe('the offered rows', () => {
    it('lists the flagged entry points first and the routes after them', () => {
        const { rows } = entryRows({ entryPoints: hits, routes });
        expect(rows.map((row) => row.name)).toEqual([
            'loadConfig',
            'createUser',
            'nowhere',
            'GET /users -> listUsers',
            'POST /orders',
        ]);
    });

    it('says which reading each row came from', () => {
        const { rows } = entryRows({ entryPoints: hits, routes });
        expect(rows[0].origin).toBe('entry point');
        expect(rows[3].origin).toBe('route (read from the source)');
        expect(rows[4].origin).toBe('route (from the index)');
    });

    it('names the file and the line where the index gave one', () => {
        const { rows } = entryRows({ entryPoints: hits, routes });
        expect(rows[0].where).toBe('src/config.ts:11');
        expect(rows[2].where).toBe('');
    });

    it('keeps a row the index placed nowhere, and gives it nothing to open', () => {
        const { rows } = entryRows({ entryPoints: hits, routes });
        expect(rows[2].name).toBe('nowhere');
        expect(rows[2].target).toBeUndefined();
        expect(rows[4].target).toBeUndefined();
    });

    it('carries enough of a row to start a walk from it', () => {
        const { rows } = entryRows({ entryPoints: hits, routes });
        expect(rows[1].target).toMatchObject({
            name: 'createUser',
            qualifiedName: 'p.src.services.userService.createUser',
            filePath: 'src/services/userService.ts',
            startLine: 23,
        });
    });

    it('shows a repeated way in once', () => {
        const { rows, total } = entryRows({ entryPoints: [...hits, hits[0]], routes: [] });
        expect(total).toBe(3);
        expect(rows).toHaveLength(3);
    });

    it('cuts a long list at its bound and says how long it was', () => {
        const many = Array.from({ length: 60 }, (_unused, index) => ({
            name: `f${index}`,
            qualifiedName: `p.f${index}`,
            kind: 'function' as const,
            filePath: 'src/a.ts',
            line: index + 1,
        }));
        const { rows, total } = entryRows({ entryPoints: many, routes: [] }, 40);
        expect(rows).toHaveLength(40);
        expect(total).toBe(60);
        expect(entryHeadline(total, rows.length)).toBe('60 ways in the index flagged, 40 shown: search for the rest');
    });

    it('has nothing to offer before the summary arrived', () => {
        expect(entryRows(undefined)).toEqual({ rows: [], total: 0 });
        expect(entryHeadline(0, 0)).toBe('the index flagged no entry point for this project');
    });

    it('counts one way in as one', () => {
        expect(entryHeadline(1, 1)).toBe('1 way in the index flagged');
        expect(entryHeadline(5, 5)).toBe('5 ways in the index flagged');
    });
});

describe('a list with no route in it', () => {
    it('says why, rather than letting the absence read as a finding', () => {
        expect(routeNote({ entryPoints: hits, routes: [] })).toBe(NO_ROUTES_NOTE);
        expect(NO_ROUTES_NOTE).toContain('the index reported none');
        expect(NO_ROUTES_NOTE).toContain('reads no source files of its own');
    });

    it('says nothing when the list does hold routes', () => {
        expect(routeNote({ entryPoints: hits, routes })).toBe('');
    });

    it('says nothing before the summary arrived, because nobody has looked yet', () => {
        expect(routeNote(undefined)).toBe('');
    });
});
