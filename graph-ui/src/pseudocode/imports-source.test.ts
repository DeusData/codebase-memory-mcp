/**
 * Woher die Imports-Gruppe ihre Liste bekommt, und was sie dabei nicht behauptet.
 *
 * Diese Datei hat kein Gegenstueck im Referenzprojekt: dort beantwortet ein
 * Theia-Backend `fileImports`, hier wird die Antwort aus zwei vorhandenen
 * Lesungen gebaut (siehe Kopf von imports-source.ts). Getestet wird deshalb
 * genau das, was diese Beschaffung selbst entscheidet:
 *
 *  1. Der Scanner findet die Formen, die die Fixture wirklich enthaelt, und
 *     traegt fuer jeden Namen die Zeile seiner Anweisung.
 *  2. Er behauptet nie, gelesen zu haben, was er nicht gelesen hat: ohne Text
 *     ist `sourceRead` falsch und jeder Eintrag stammt aus einer Index-Kante.
 *  3. Die Zuordnung Spezifizierer zu indizierter Datei ist ein Vergleich gegen
 *     die Kanten des Index, nie eine Erfindung: ein Paketname bekommt keinen
 *     Pfad.
 */

import { readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

import { describe, expect, it } from 'vitest';

import type { ModuleDependencyGraph } from '../core/intelligence-provider';
import {
    MAX_IMPORTS_PER_FILE,
    fileImportsFor,
    isImportSource,
    readImportStatements,
    resolveImportTarget,
} from './imports-source';

const FIXTURE = join(
    dirname(fileURLToPath(import.meta.url)),
    '..',
    '..',
    'fixtures',
    'atlas-sample',
    'src',
    'services',
    'userService.ts',
);

const USER_SERVICE = 'src/services/userService.ts';
const SOURCE = readFileSync(FIXTURE, 'utf8');

const CONTEXT = { filePath: USER_SERVICE, providerId: 'cbm', engineGeneration: 1 };

/** Ein Provider-Ausschnitt aus erfundenen Kanten. Kein Server, kein Transport. */
function source(edges: { from: string; to: string }[], truncated = false) {
    return {
        id: 'cbm',
        moduleDependencies: async (): Promise<ModuleDependencyGraph> => ({ edges, truncated }),
    };
}

const SAMPLE_EDGES = [
    { from: USER_SERVICE, to: 'src/repo/db.ts' },
    { from: USER_SERVICE, to: 'src/types.ts' },
    { from: USER_SERVICE, to: 'src/util/validate.ts' },
    { from: 'src/server.ts', to: USER_SERVICE },
];

describe('reading the import statements off a file', () => {
    const entries = readImportStatements(SOURCE, CONTEXT);

    it('finds every name the fixture pulls in, in statement order', () => {
        expect(entries.map((entry) => entry.name)).toEqual([
            'insert', 'query', 'Row', 'UserEntity', 'User', 'ValidationError', 'validateUser',
        ]);
    });

    it('gives every name the line of the statement that binds it', () => {
        const byName = new Map(entries.map((entry) => [entry.name, entry.line]));
        expect(byName.get('insert')).toBe(4);
        expect(byName.get('query')).toBe(4);
        expect(byName.get('UserEntity')).toBe(5);
        expect(byName.get('User')).toBe(6);
        expect(byName.get('validateUser')).toBe(7);
    });

    it('reads a type-only import as an import, because a signature naming it is a use', () => {
        expect(entries.find((entry) => entry.name === 'User')?.module).toBe('../types');
    });

    it('cites itself as a reading of the text and never as a finding of the index', () => {
        for (const entry of entries) {
            expect(entry.origin).toBe('source');
            expect(entry.evidence[0].source).toBe('source-text');
            expect(entry.evidence[0].relation).toBe('import-statement');
            expect(entry.evidence[0].file).toBe(USER_SERVICE);
        }
    });

    it('keeps the exported name and the local binding apart on an alias', () => {
        const aliased = readImportStatements("import { validateUser as check } from '../util/validate';", CONTEXT);
        expect(aliased).toEqual([expect.objectContaining({ name: 'validateUser', alias: 'check' })]);
    });

    it('marks a namespace binding as one rather than as a name to look for', () => {
        const namespaced = readImportStatements("import * as db from '../repo/db';", CONTEXT);
        expect(namespaced[0]).toMatchObject({ alias: 'db', namespace: true });
        expect(namespaced[0].name).toBeUndefined();
    });

    it('records a side-effect import as an entry that binds nothing', () => {
        const sideEffect = readImportStatements("import './register';", CONTEXT);
        expect(sideEffect).toHaveLength(1);
        expect(sideEffect[0].name).toBeUndefined();
        expect(sideEffect[0].module).toBe('./register');
    });

    it('does not read the front of a real statement as a second, side-effect one', () => {
        expect(readImportStatements("import { a } from 'm';", CONTEXT)).toHaveLength(1);
    });

    it('keeps a default binding under the name the file gives it', () => {
        const mixed = readImportStatements("import fs, { readFile } from 'node:fs';", CONTEXT);
        expect(mixed.map((entry) => entry.name)).toEqual(['fs', 'readFile']);
    });

    it('reads nothing at all from a file whose form it does not know', () => {
        expect(readImportStatements(SOURCE, { ...CONTEXT, filePath: 'src/main.rb' })).toEqual([]);
        expect(isImportSource('src/main.rb')).toBe(false);
        expect(isImportSource(USER_SERVICE)).toBe(true);
    });
});

describe('which indexed file a specifier means', () => {
    const targets = ['src/repo/db.ts', 'src/types.ts', 'src/util/validate.ts', 'src/repo/index.ts'];

    it('matches a relative specifier against the edges the index recorded', () => {
        expect(resolveImportTarget(USER_SERVICE, '../repo/db', targets)).toBe('src/repo/db.ts');
        expect(resolveImportTarget(USER_SERVICE, '../types', targets)).toBe('src/types.ts');
    });

    it('finds a directory import through its index file', () => {
        expect(resolveImportTarget(USER_SERVICE, '../repo', targets)).toBe('src/repo/index.ts');
    });

    it('gives a package name no path at all, because it resolves outside the workspace', () => {
        expect(resolveImportTarget(USER_SERVICE, 'node:fs', targets)).toBeUndefined();
        expect(resolveImportTarget(USER_SERVICE, 'express', targets)).toBeUndefined();
    });

    it('gives a relative specifier the index never recorded no path either', () => {
        expect(resolveImportTarget(USER_SERVICE, '../nowhere/at/all', targets)).toBeUndefined();
    });
});

describe('the whole answer for one file', () => {
    it('joins the names from the text to the dependencies from the index', async () => {
        const dto = await fileImportsFor(source(SAMPLE_EDGES), '/workspace', {
            filePath: USER_SERVICE,
            source: SOURCE,
        });
        expect(dto.sourceRead).toBe(true);
        expect(dto.indexedTargets).toEqual(['src/repo/db.ts', 'src/types.ts', 'src/util/validate.ts']);
        expect(dto.entries.map((entry) => entry.name)).toEqual([
            'insert', 'query', 'Row', 'UserEntity', 'User', 'ValidationError', 'validateUser',
        ]);
        expect(dto.entries.every((entry) => entry.targetPath !== undefined)).toBe(true);
        expect(dto.truncated).toBe(false);
    });

    it('leaves out the edges of other files, which are not this file\'s imports', async () => {
        const dto = await fileImportsFor(source(SAMPLE_EDGES), '/workspace', {
            filePath: USER_SERVICE,
            source: SOURCE,
        });
        expect(dto.indexedTargets).not.toContain(USER_SERVICE);
    });

    it('keeps a dependency the text did not name, as the index-only entry it is', async () => {
        const dto = await fileImportsFor(
            source([...SAMPLE_EDGES, { from: USER_SERVICE, to: 'src/generated/schema.ts' }]),
            '/workspace',
            { filePath: USER_SERVICE, source: SOURCE },
        );
        const extra = dto.entries.filter((entry) => entry.origin === 'index');
        expect(extra.map((entry) => entry.targetPath)).toEqual(['src/generated/schema.ts']);
        expect(extra[0].evidence[0].source).toBe('graph-edge');
        expect(extra[0].name).toBeUndefined();
    });

    it('says the text was not read rather than reporting an empty file', async () => {
        const dto = await fileImportsFor(source(SAMPLE_EDGES), '/workspace', { filePath: USER_SERVICE });
        expect(dto.sourceRead).toBe(false);
        expect(dto.entries.every((entry) => entry.origin === 'index')).toBe(true);
        expect(dto.entries).toHaveLength(3);
    });

    it('carries the provider\'s own bound through instead of presenting a floor as a total', async () => {
        const dto = await fileImportsFor(source(SAMPLE_EDGES, true), '/workspace', {
            filePath: USER_SERVICE,
            source: SOURCE,
        });
        expect(dto.truncated).toBe(true);
    });

    it('answers rather than throwing when the analysis is not there', async () => {
        const broken = {
            id: 'cbm',
            moduleDependencies: async (): Promise<ModuleDependencyGraph> => {
                throw new Error('engine unavailable');
            },
        };
        const dto = await fileImportsFor(broken, '/workspace', { filePath: USER_SERVICE, source: SOURCE });
        expect(dto.indexedTargets).toEqual([]);
        expect(dto.entries).toHaveLength(7);
        expect(dto.entries.every((entry) => entry.targetPath === undefined)).toBe(true);
    });

    it('stops at its own bound and says so', async () => {
        const many = Array.from(
            { length: MAX_IMPORTS_PER_FILE + 3 },
            (_unused, index) => `import { n${index} } from './m${index}';`,
        ).join('\n');
        const dto = await fileImportsFor(source([]), '/workspace', {
            filePath: USER_SERVICE,
            source: many,
        });
        expect(dto.entries).toHaveLength(MAX_IMPORTS_PER_FILE);
        expect(dto.truncated).toBe(true);
    });

    it('withholds file facts when one callable declaration cannot be resolved', async () => {
        const partial = {
            ...source([]),
            resolveSymbolAt: async (_root: string, _path: string, line: number) => line === 1
                ? {
                    kind: 'ok' as const,
                    symbol: {
                        name: 'first',
                        qualifiedName: 'p.src.first',
                        kind: 'function' as const,
                        uri: 'file:///workspace/src/sample.ts',
                        range: {
                            start: { line: 0, character: 0 },
                            end: { line: 0, character: 25 },
                        },
                    },
                    enclosing: [],
                }
                : { kind: 'no-symbol-at-line' as const, filePath: 'src/sample.ts' },
            getFacts: async () => ({}),
            getSnippet: async () => 'export function first() {}',
        };
        const dto = await fileImportsFor(partial, '/workspace', {
            filePath: 'src/sample.ts',
            source: 'export function first() {}\nexport function second() {}',
        });
        expect(dto.fileIrs).toBeUndefined();
    });
});
