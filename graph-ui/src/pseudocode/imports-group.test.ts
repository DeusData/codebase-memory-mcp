/*
 * Portiert am 2026-08-29 aus CodeAtlasIDE,
 * theia-extensions/codeatlas-views/test/imports-group.test.ts (275 Zeilen).
 * Unveraendert bis auf die Importpfade und darauf, dass FileImportRef und
 * FileImportsDto hier aus ./imports-group kommen (siehe Kopf jener Datei).
 */

/**
 * What the imports group is allowed to say about an import and its full file.
 *
 * The suite is organised around the one mistake this feature can make that
 * nobody would catch by looking at the screen. "unused import" is a sentence a
 * reader will believe, it is what every linter they have ever used says, and
 * CodeAtlas is in no position to say it from one focused symbol: it needs a
 * complete callable reading of the file. So the tests are mostly about when the
 * product must decline to answer.
 *
 * Three kinds of assertion:
 *
 *  1. A positive match is found, and it is found in the right family with the
 *     right citation behind it. A `used` marker with no evidence is decoration.
 *  2. A negative answer is `unused` only when every family the check would have
 *     run against was actually answered, and `unknown` the moment one was not.
 *     This is the difference between a finding and a gap, and it is asserted
 *     from both directions.
 *  3. The shapes that name nothing (a namespace binding, a side-effect import,
 *     an entry recovered from an edge alone) are never called unused, because
 *     there was never a name to look for.
 *
 * The IR is the committed recording of what the 0.9.0 provider actually returns
 * for `createUser` in the frozen sample workspace, which is what makes the
 * central case real rather than arranged: `insert` is imported and called,
 * `query` is imported on the very same line and belongs to `listUsers` four
 * lines up.
 */

import { readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

import { describe, expect, it } from 'vitest';

import type { Evidence, KnowledgeState, SemanticIR } from '../core/semantic-ir';

import type { FileImportRef, FileImportsDto } from './imports-group';
import { IMPORTS_GROUP_CAP, buildImportsGroup } from './imports-group';

const IR_FIXTURES = join(dirname(fileURLToPath(import.meta.url)), '..', 'twin', '__fixtures__');

function ir(name: string): SemanticIR {
    return JSON.parse(readFileSync(join(IR_FIXTURES, `ir-${name}.json`), 'utf8')) as SemanticIR;
}

const CREATE_USER = ir('createUser');
const LIST_USERS = ir('listUsers');

const URI = CREATE_USER.symbol.uri;

const IMPORT_CITATION: Evidence = {
    source: 'source-text',
    relation: 'import-statement',
    file: 'src/services/userService.ts',
    range: { startLine: 4, endLine: 4 },
    strategy: 'import-statement-read',
    engineGeneration: 1,
    providerId: 'cbm',
};

function entry(over: Partial<FileImportRef>): FileImportRef {
    return {
        module: '../repo/db',
        line: 4,
        origin: 'source',
        evidence: [IMPORT_CITATION],
        ...over,
    };
}

function dto(entries: FileImportRef[], over: Partial<FileImportsDto> = {}): FileImportsDto {
    return {
        entries,
        truncated: false,
        indexedTargets: ['src/repo/db.ts', 'src/types.ts', 'src/util/validate.ts'],
        sourceRead: true,
        ...over,
    };
}

/** The four imports of the fixture's `userService.ts`, as the provider reports them. */
const USER_SERVICE_IMPORTS: FileImportRef[] = [
    entry({ name: 'insert', targetPath: 'src/repo/db.ts' }),
    entry({ name: 'query', targetPath: 'src/repo/db.ts' }),
    entry({ name: 'Row', targetPath: 'src/repo/db.ts' }),
    entry({ name: 'UserEntity', module: '../types', line: 5, targetPath: 'src/types.ts' }),
    entry({ name: 'User', module: '../types', line: 6, targetPath: 'src/types.ts' }),
    entry({ name: 'ValidationError', module: '../util/validate', line: 7, targetPath: 'src/util/validate.ts' }),
    entry({ name: 'validateUser', module: '../util/validate', line: 7, targetPath: 'src/util/validate.ts' }),
];

/** One IR with a family forced into a state the recording never has. */
function withState(document: SemanticIR, family: 'calls' | 'throws' | 'reads', state: KnowledgeState): SemanticIR {
    return { ...document, [family]: { ...document[family], state } };
}

function group(entries: FileImportRef[], irs: readonly SemanticIR[] = [CREATE_USER], cap?: number) {
    return buildImportsGroup({ imports: dto(entries, { fileIrs: [...irs] }), irs, uri: URI, cap });
}

describe('what a complete file reading demonstrably uses', () => {
    const built = group(USER_SERVICE_IMPORTS);
    const byLabel = new Map(built.entries.map((item) => [item.label, item]));

    it('calls an import used when a recorded call names it', () => {
        expect(byLabel.get('insert')?.usage).toBe('used');
        expect(byLabel.get('insert')?.usedBy).toBe('calls');
        expect(byLabel.get('validateUser')?.usage).toBe('used');
    });

    it('counts a construction as a use, because the index records it as a call', () => {
        expect(byLabel.get('UserEntity')?.usage).toBe('used');
        expect(byLabel.get('UserEntity')?.usedBy).toBe('calls');
    });

    it('counts a type the signature names as a use', () => {
        expect(byLabel.get('User')?.usage).toBe('used');
        expect(byLabel.get('User')?.usedBy).toBe('typeRefs');
    });

    it('carries the citation of the fact that proved the use, beside the import citation', () => {
        const used = byLabel.get('insert');
        expect(used?.evidence).toHaveLength(2);
        expect(used?.evidence[0]).toEqual(IMPORT_CITATION);
        expect(used?.evidence[1].relation).toBe('invocation');
    });

    it('says nothing about use in the citations of an import it could not match', () => {
        expect(byLabel.get('query')?.evidence).toEqual([IMPORT_CITATION]);
    });
});

describe('what a complete file reading does not use, said as a statement about the index', () => {
    const built = group(USER_SERVICE_IMPORTS);
    const byLabel = new Map(built.entries.map((item) => [item.label, item]));

    it('marks an import unused only when the complete reading never names it', () => {
        expect(byLabel.get('query')?.usage).toBe('unused');
        expect(byLabel.get('Row')?.usage).toBe('unused');
    });

    it('words the sentence about the whole file', () => {
        const text = byLabel.get('query')?.text ?? '';
        expect(text).toContain('imported here, not used anywhere in this file as far as the index shows');
        expect(text.toLowerCase()).not.toContain('unused import');
    });

    it('finds the same import used once the complete reading includes its user', () => {
        const forListUsers = group(USER_SERVICE_IMPORTS, [LIST_USERS]);
        const byName = new Map(forListUsers.entries.map((item) => [item.label, item]));
        expect(byName.get('query')?.usage).toBe('used');
        expect(byName.get('insert')?.usage).toBe('unused');
    });

    it('judges a class against its members, so a method\'s call counts for the class', () => {
        const forClass = group(USER_SERVICE_IMPORTS, [LIST_USERS, CREATE_USER]);
        const byName = new Map(forClass.entries.map((item) => [item.label, item]));
        expect(byName.get('query')?.usage).toBe('used');
        expect(byName.get('insert')?.usage).toBe('used');
        expect(byName.get('Row')?.usage).toBe('unused');
    });
});

describe('when the check cannot be made, nothing is claimed', () => {
    it('does not turn the focused symbol into a fallback file reading', () => {
        const built = buildImportsGroup({ imports: dto(USER_SERVICE_IMPORTS), irs: [CREATE_USER], uri: URI });
        expect(built.entries.every((item) => item.usage === 'unknown')).toBe(true);
        expect(built.unused).toBe(0);
        expect(built.entries[0].text).toContain('cannot tell whether this file uses it');
        expect(built.entries[0].note).toContain('complete answered reading for every callable');
    });

    it('refuses to call anything unused while a family is not indexed', () => {
        for (const state of ['notIndexed', 'unsupported', 'unknown', 'ambiguous'] as const) {
            const built = group(USER_SERVICE_IMPORTS, [withState(CREATE_USER, 'reads', state)]);
            const byLabel = new Map(built.entries.map((item) => [item.label, item]));
            expect(byLabel.get('query')?.usage, state).toBe('unknown');
            expect(built.unused, state).toBe(0);
        }
    });

    it('still reports a positive match while another family is missing', () => {
        const built = group(USER_SERVICE_IMPORTS, [withState(CREATE_USER, 'reads', 'notIndexed')]);
        const byLabel = new Map(built.entries.map((item) => [item.label, item]));
        expect(byLabel.get('insert')?.usage).toBe('used');
    });

    it('treats a provider that omits type references as a reason not to answer', () => {
        const withoutTypes: SemanticIR = { ...CREATE_USER, typeRefs: undefined };
        const built = group(USER_SERVICE_IMPORTS, [withoutTypes]);
        const byLabel = new Map(built.entries.map((item) => [item.label, item]));
        expect(byLabel.get('query')?.usage).toBe('unknown');
        expect(byLabel.get('User')?.usage).toBe('unknown');
    });

    it('claims nothing at all when no symbol\'s facts were available', () => {
        const built = group(USER_SERVICE_IMPORTS, []);
        expect(built.entries.every((item) => item.usage === 'unknown')).toBe(true);
    });

    it('declines to judge a namespace binding, whose members the index records by their own names', () => {
        const built = group([entry({ alias: 'db', namespace: true })]);
        expect(built.entries[0].usage).toBe('unknown');
        expect(built.entries[0].label).toBe('db');
        expect(built.entries[0].text).toContain('the whole module under one name');
    });

    it('declines to judge a side-effect import, which binds no name to look for', () => {
        const built = group([entry({ module: './register', line: 2 })]);
        expect(built.entries[0].usage).toBe('unknown');
        expect(built.entries[0].text).toContain('pulled in for what it does when the file loads');
    });

    it('declines to judge an entry recovered from an import edge alone', () => {
        const built = buildImportsGroup({
            imports: dto(
                [{ module: 'src/repo/db.ts', targetPath: 'src/repo/db.ts', origin: 'index', evidence: [] }],
                { sourceRead: false },
            ),
            irs: [CREATE_USER],
            uri: URI,
        });
        expect(built.sourceRead).toBe(false);
        expect(built.entries[0].usage).toBe('unknown');
        expect(built.entries[0].sourceRef).toBeUndefined();
    });
});

describe('where a reader is sent, and what the group says about itself', () => {
    it('points every text-read entry at its own statement in the anchor file', () => {
        const built = group(USER_SERVICE_IMPORTS);
        expect(built.entries[0].sourceRef).toEqual({ uri: URI, line: 4 });
        expect(built.entries[6].sourceRef).toEqual({ uri: URI, line: 7 });
    });

    it('keeps the exported name and the local one both visible on an aliased import', () => {
        const built = group([entry({ name: 'validateUser', alias: 'check', module: '../util/validate' })]);
        expect(built.entries[0].label).toBe('validateUser as check');
        expect(built.entries[0].usage).toBe('used');
    });

    it('tallies the three statuses over the whole list, not over the capped one', () => {
        const built = group(USER_SERVICE_IMPORTS, [CREATE_USER], 2);
        expect(built.used).toBe(5);
        expect(built.unused).toBe(2);
        expect(built.unknown).toBe(0);
        expect(built.tally).toBe('5 used in this file, 2 not used in it, 0 that CodeAtlas cannot check.');
    });

    it('says how many entries the cap left out rather than trailing off', () => {
        const built = group(USER_SERVICE_IMPORTS, [CREATE_USER], 2);
        expect(built.entries).toHaveLength(2);
        expect(built.hidden).toBe(5);
        expect(built.cappedNote).toBe('and 5 more imports not listed: this group shows the first 2');
    });

    it('says nothing about a cap that cut nothing', () => {
        const built = group(USER_SERVICE_IMPORTS);
        expect(built.hidden).toBe(0);
        expect(built.cappedNote).toBeUndefined();
        expect(USER_SERVICE_IMPORTS.length).toBeLessThan(IMPORTS_GROUP_CAP);
    });

    it('gives an empty file a group rather than nothing, so the panel can say so', () => {
        const built = group([]);
        expect(built.entries).toEqual([]);
        expect(built.hidden).toBe(0);
        expect(built.tally).toContain('0 used in this file');
    });

    it('marks every entry in words as well as in a machine-readable status', () => {
        const built = group(USER_SERVICE_IMPORTS);
        const markers = new Set(built.entries.map((item) => item.marker));
        expect(markers).toEqual(new Set(['used here', 'not used here']));
        expect(built.entries.every((item) => item.note.length > 0)).toBe(true);
    });
});
