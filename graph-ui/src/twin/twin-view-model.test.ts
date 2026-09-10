/*
 * Die Lesart der IR unter dem Render-Modell: welche Zeile welchen Beleg traegt,
 * wohin sie fuehrt, und welcher Zustand ueber einer Sektion steht.
 *
 * Der Trennstrich zwischen `siteLine` und `target` ist der Fehler, gegen den
 * dieses Modul geschnitten wurde: ein Aufruf von `validateUser` in Zeile 24 von
 * userService.ts ist in Zeile 19 von validate.ts deklariert, und beide Zahlen
 * sind wahr ueber verschiedene Dateien.
 */

import { describe, expect, it } from 'vitest';

import { buildImportsGroup } from '../pseudocode/imports-group';
import { CREATE_USER_IR } from '../test-support/twin-fixtures';
import {
    buildSections,
    displayFile,
    evidenceFor,
    importsSection,
    locationLabel,
    runtimeSection,
    stepTarget,
    worstState,
} from './twin-view-model';

const sections = buildSections(CREATE_USER_IR);
const section = (name: string) => sections.find((entry) => entry.name === name)!;

describe('worstState', () => {
    it('nimmt den am wenigsten vertrauenswuerdigen Zustand', () => {
        expect(worstState(['known', 'inferred'])).toBe('inferred');
        expect(worstState(['inferred', 'unsupported'])).toBe('unsupported');
        expect(worstState(['unsupported', 'notIndexed', 'unknown'])).toBe('unknown');
    });

    it('ist ohne Zustaende `known`, weil eine leere Sammlung keine Luecke ist', () => {
        expect(worstState([])).toBe('known');
    });
});

describe('displayFile und locationLabel', () => {
    it('nennt die Datei und die Zeile', () => {
        expect(locationLabel('file:///workspace/src/util/validate.ts', 19)).toBe('validate.ts:19');
    });

    it('nennt nur die Datei, wenn keine Zeile aufgezeichnet ist', () => {
        expect(locationLabel('file:///workspace/src/util/validate.ts', undefined)).toBe('validate.ts');
    });

    it('ist undefined ohne Datei, statt eine leere Beschriftung zu bauen', () => {
        expect(displayFile(undefined)).toBeUndefined();
        expect(locationLabel(undefined, 19)).toBeUndefined();
    });
});

describe('stepTarget', () => {
    it('fuehrt zur Deklaration des Ziels und nicht zur Aufrufstelle', () => {
        const call = CREATE_USER_IR.steps.value[0];
        expect(call.line).toBe(24);
        const target = stepTarget(call);
        expect(target?.uri).toContain('validate.ts');
        // 1-basierte Graph-Zeile 19 wird zu 0-basierter Editorzeile 18.
        expect(target?.range.start.line).toBe(18);
        expect(target?.qualifiedName).toBe(call.targetQualifiedName);
    });

    it('hat kein Ziel, wenn der Aufruf keine Zieldatei kennt', () => {
        expect(stepTarget({ targetName: 'irgendwas' })).toBeUndefined();
    });
});

describe('buildSections', () => {
    it('liefert jede Sektion, in Lesereihenfolge, ob gefuellt oder nicht', () => {
        expect(sections.map((entry) => entry.name)).toEqual([
            'purpose',
            'steps',
            'callers',
            'state',
            'errors',
            'effects',
            'tests',
            'risks',
        ]);
    });

    it('traegt auf jeder Schritt-Zeile die Aufrufzeile im eigenen File', () => {
        expect(section('steps').rows.map((row) => row.siteLine)).toEqual([24, 27, 29, 29, 30, 35]);
    });

    it('markiert einen Aufrufer, den der Index als Testcode gekennzeichnet hat', () => {
        const badges = section('callers').rows.map((row) => row.badge?.text);
        expect(badges).toContain('test');
    });

    it('haelt die drei Datenfamilien getrennt unter eigenen Gruppen', () => {
        const groups = new Set(section('state').rows.map((row) => row.group));
        expect(groups.has('Reads from the environment')).toBe(true);
        expect(groups.has('Uses types')).toBe(true);
    });

    it('gibt jeder Zeile eine Adresse fuer ihren Beleg', () => {
        expect(section('steps').rows.map((row) => row.factPath)).toEqual([
            'steps[0]',
            'steps[1]',
            'steps[2]',
            'steps[3]',
            'steps[4]',
            'steps[5]',
        ]);
    });

    /*
     * Befund 1 des unabhaengigen Audits vom 2026-08-29.
     *
     * Der Leer-Satz der Aufrufer-Sektion behauptete absolut ueber den
     * Arbeitsbereich ("Nothing in the indexed workspace calls this symbol"),
     * und das Audit hat ihn an einer Fixture mit Registry-Versand widerlegt.
     * Geprueft wird darum nicht ein Wortlaut, sondern die Eigenschaft, wegen
     * der er geaendert wurde: jeder Leer-Satz dieses Panels nennt den Index
     * als Quelle der Luecke und keiner den Arbeitsbereich.
     */
    it('schreibt jede Leerstelle dem Index zu und nicht dem Arbeitsbereich', () => {
        const bare = {
            ...CREATE_USER_IR,
            calledBy: { ...CREATE_USER_IR.calledBy, value: [], evidence: [] },
            steps: { ...CREATE_USER_IR.steps, value: [], evidence: [] },
            throws: { ...CREATE_USER_IR.throws, value: [], evidence: [] },
        };
        const built = buildSections(bare);
        const callers = built.find((entry) => entry.name === 'callers')!;
        expect(callers.populated).toBe(false);
        expect(callers.emptyText).toMatch(/index/i);
        expect(callers.emptyText).not.toMatch(/indexed workspace/i);
        for (const name of ['callers', 'steps', 'errors']) {
            const found = built.find((entry) => entry.name === name)!;
            expect(found.emptyText, `${name}: ${found.emptyText}`).toMatch(/index/i);
        }
    });

    it('sagt bei fehlenden Tests einen Befund und nie "nicht getestet"', () => {
        const noTests = {
            ...CREATE_USER_IR,
            tests: { ...CREATE_USER_IR.tests, value: [], evidence: [] },
        };
        const tests = buildSections(noTests).find((entry) => entry.name === 'tests')!;
        expect(tests.populated).toBe(false);
        expect(tests.emptyText).toContain('No test callers found');
        expect(tests.emptyText).toContain('inferred from callers');
        expect(tests.emptyText).not.toMatch(/not tested/i);
    });
});

describe('evidenceFor', () => {
    it('liefert die Belege einer ganzen Familie', () => {
        expect(evidenceFor(CREATE_USER_IR, 'steps')).toHaveLength(CREATE_USER_IR.steps.evidence.length);
    });

    it('liefert den Beleg einer Zeile', () => {
        const [entry] = evidenceFor(CREATE_USER_IR, 'steps[0]');
        expect(entry.file).toContain('validate.ts');
        expect(entry.providerId.length).toBeGreaterThan(0);
    });

    it('liefert nichts, wenn Werte und Belege nicht im Gleichschritt sind', () => {
        const skewed = {
            ...CREATE_USER_IR,
            steps: { ...CREATE_USER_IR.steps, evidence: CREATE_USER_IR.steps.evidence.slice(0, 1) },
        };
        expect(evidenceFor(skewed, 'steps[0]')).toEqual([]);
    });

    it('liefert nichts fuer eine Adresse, die keine ist', () => {
        expect(evidenceFor(CREATE_USER_IR, '')).toEqual([]);
        expect(evidenceFor(CREATE_USER_IR, 'nichtsdergleichen')).toEqual([]);
    });
});

describe('runtimeSection', () => {
    it('ist undefined, wenn die IR gar keinen runtime-Fakt traegt', () => {
        expect(CREATE_USER_IR.runtime).toBeUndefined();
        expect(runtimeSection(CREATE_USER_IR)).toBeUndefined();
    });

    it('unterscheidet "nichts importiert" von "importiert und nichts getroffen"', () => {
        const imported = {
            ...CREATE_USER_IR,
            runtime: { state: 'known' as const, value: [], evidence: [] },
        };
        const built = runtimeSection(imported);
        expect(built?.populated).toBe(false);
        expect(built?.emptyText).toContain('A recording has been imported');
        expect(built?.stateNote).toContain('not from the index');
    });

    it('markiert einen beobachteten Aufruf, den der Index nicht kennt', () => {
        const imported = {
            ...CREATE_USER_IR,
            runtime: {
                state: 'known' as const,
                value: [{ targetName: 'sendMail', count: 3, unexpected: true }],
                evidence: [],
            },
        };
        const built = runtimeSection(imported);
        expect(built?.rows[0].badge?.text).toBe('not in the index');
        expect(built?.rows[0].extras).toEqual(['3x']);
    });
});

/*
 * Die Imports-Sektion: die in W2b deklarierte Auslassung, seit W4c wieder da.
 *
 * Das Referenzprojekt hat fuer `importsSection` keinen eigenen Unit-Test; es
 * prueft die Gruppe (imports-group.test.ts, hier portiert) und laesst die
 * Sektion darueber vom Panel zeichnen. Diese Faelle sind darum neu geschrieben
 * und decken genau das ab, was die Sektion selbst entscheidet: was eine Zeile
 * traegt, wohin sie fuehrt, welcher Zustand ueber ihr steht und welche Saetze
 * darunter.
 */
describe('importsSection', () => {
    const group = buildImportsGroup({
        imports: {
            entries: [
                {
                    name: 'insert',
                    module: '../repo/db',
                    line: 4,
                    targetPath: 'src/repo/db.ts',
                    origin: 'source',
                    evidence: [],
                },
                {
                    name: 'query',
                    module: '../repo/db',
                    line: 4,
                    targetPath: 'src/repo/db.ts',
                    origin: 'source',
                    evidence: [],
                },
            ],
            truncated: false,
            indexedTargets: ['src/repo/db.ts'],
            sourceRead: true,
            fileIrs: [CREATE_USER_IR],
        },
        irs: [CREATE_USER_IR],
        uri: CREATE_USER_IR.symbol.uri,
    });
    const built = importsSection(group);

    it('zeichnet jede Zeile als Chip, weil Importe eine Menge sind und keine Folge', () => {
        expect(built.rows.every((row) => row.display === 'chip')).toBe(true);
        expect(built.rows.map((row) => row.label)).toEqual(['insert', 'query']);
    });

    it('traegt den Marker als Abzeichen und seinen ganzen Satz als Tooltip', () => {
        expect(built.rows[0].badge?.text).toBe('used here');
        expect(built.rows[1].badge?.text).toBe('not used here');
        expect(built.rows[1].badge?.tooltip.length).toBeGreaterThan(20);
    });

    it('fuehrt eine Zeile zu ihrer eigenen Anweisung, nicht zur importierten Datei', () => {
        expect(built.rows[0].target?.uri).toBe(CREATE_USER_IR.symbol.uri);
        expect(built.rows[0].target?.range.start.line).toBe(3);
        // Kein nodeId: das ist ein Ort in einer Datei, kein aufgeloester Knoten.
        expect(built.rows[0].target?.nodeId).toBeUndefined();
    });

    it('steht im DATA-Block und sagt, dass es eine Regel ueber Fakten ist', () => {
        expect(built.block).toBe('data');
        expect(built.state).toBe('inferred');
        expect(built.stateNote).toContain('The dependency comes from the index');
    });

    it('schreibt die Bilanz ueber die Zeilen und die Herkunft darunter', () => {
        expect(built.text).toBe('1 used in this file, 1 not used in it, 0 that CodeAtlas cannot check.');
        expect(built.note).toContain('The dependency comes from the index');
        expect(built.populated).toBe(true);
    });

    it('sagt bei einer ungelesenen Datei, dass sie ungelesen ist', () => {
        const unread = importsSection(buildImportsGroup({
            imports: {
                entries: [{ module: 'src/repo/db.ts', targetPath: 'src/repo/db.ts', origin: 'index', evidence: [] }],
                truncated: false,
                indexedTargets: ['src/repo/db.ts'],
                sourceRead: false,
            },
            irs: [CREATE_USER_IR],
            uri: CREATE_USER_IR.symbol.uri,
        }));
        expect(unread.note).toContain('could not read this file');
        expect(unread.rows[0].target).toBeUndefined();
    });

    it('gibt einer Datei ohne Importe eine Ueberschrift und einen Satz statt nichts', () => {
        const empty = importsSection(buildImportsGroup({
            imports: { entries: [], truncated: false, indexedTargets: [], sourceRead: true },
            irs: [CREATE_USER_IR],
            uri: CREATE_USER_IR.symbol.uri,
        }));
        expect(empty.rows).toEqual([]);
        expect(empty.populated).toBe(false);
        expect(empty.emptyText.length).toBeGreaterThan(20);
        expect(empty.text).toBeUndefined();
        expect(empty.note).toBeUndefined();
    });
});
