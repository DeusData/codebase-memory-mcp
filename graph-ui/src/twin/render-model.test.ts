/*
 * Was der Tiefenregler aendern darf und was nicht.
 *
 * Portiert aus CodeAtlasIDE,
 * theia-extensions/codeatlas-views/test/render-model.test.ts. Ein Unterschied,
 * und er ist Absicht: das Referenzprojekt fuehrt eine committete
 * Snapshot-Datei, die vierzig Renderergebnisse festhaelt. Die kommt hier NICHT
 * mit. Ein Snapshot ist nur so viel wert wie der Reviewer, der seinen Diff
 * annimmt; ohne den Review-Weg des anderen Projekts waere er eine Datei, die
 * bei jeder Aenderung neu geschrieben und nie gelesen wird. Stattdessen sind
 * die zentralen Erwartungen hier direkt festgenagelt: dieselben Aussagen, nur
 * so geschrieben, dass beim Brechen dasteht, WAS sich geaendert hat.
 *
 * Zwei der Zusicherungen sind die Behauptung des Produkts und nicht sein
 * Verhalten: die Erzaehl-Tiefe darf keinen qualifizierten Namen und keine
 * Konfidenz zeigen, und eine Linse, die der Leser ausgeschaltet hat, darf ihre
 * Sektion nicht zuruecklassen.
 */

import { describe, expect, it } from 'vitest';

import {
    ALL_FACETS,
    CORE_FACETS,
    CREATE_USER_IR,
    NARROW_FACETS,
    QUALIFIED_NAME,
    QUALIFIED_SHAPE,
    presentation,
    sectionNames,
} from '../test-support/twin-fixtures';
import { buildImportsGroup } from '../pseudocode/imports-group';
import { Facet } from './presentation-profile';
import type { DepthLevel } from './presentation-profile';
import {
    buildTwinViewModel,
    topFacts,
    visibleSections,
    visibleTextOf,
    withImportsSection,
} from './render-model';

describe('buildTwinViewModel: eine IR, fuenf Leser', () => {
    it('nennt seine Bauform an jeder Raste ehrlich', () => {
        const modes = ([0, 1, 2, 3, 4] as const).map(
            (depth) => buildTwinViewModel(CREATE_USER_IR, presentation(depth, CORE_FACETS)).mode,
        );
        expect(modes).toEqual(['prose', 'guided', 'sections', 'cost', 'ground']);
    });

    it('rendert auf jeder Stufe etwas, statt leer zu bleiben', () => {
        for (const depth of [0, 1, 2, 3, 4] as const) {
            const text = visibleTextOf(buildTwinViewModel(CREATE_USER_IR, presentation(depth, CORE_FACETS)));
            expect(text.trim().length, `Stufe ${depth} rendert nichts`).toBeGreaterThan(0);
        }
    });

    it('stellt jeder Stufe ihre eigene Frage voran', () => {
        const questions = ([0, 1, 2, 3, 4] as const).map(
            (depth) => buildTwinViewModel(CREATE_USER_IR, presentation(depth, CORE_FACETS)).question,
        );
        // Fuenf verschiedene Fragen, und keine leer: die Frage ist das eine
        // Element, das jede Stufe hat, und der erste Satz, den ein Leser sieht.
        expect(new Set(questions).size).toBe(5);
        for (const question of questions) {
            expect(question.length).toBeGreaterThan(0);
        }
    });

    /*
     * AC1 dieses Zyklus, als Zusicherung statt als Zusage.
     *
     * Der Nutzerbefund lautete "ich seh gar keine Aenderungen", und die einzige
     * Antwort darauf, die etwas wert ist, ist eine Messung: derselbe Text an
     * zwei Rasten waere ein Regler, der nichts tut, und ein Beweislauf im
     * Browser findet das spaeter und teurer als diese Zeile hier.
     */
    it('sagt an keinen zwei Stufen dasselbe', () => {
        const texts = ([0, 1, 2, 3, 4] as const).map(
            (depth) => visibleTextOf(buildTwinViewModel(CREATE_USER_IR, presentation(depth, CORE_FACETS))),
        );
        expect(new Set(texts).size).toBe(5);
    });
});

describe('die Erzaehl-Tiefe sagt nichts, wonach niemand gefragt hat', () => {
    const model = buildTwinViewModel(CREATE_USER_IR, presentation(0, ALL_FACETS));
    /**
     * Was der Leser sieht. Nicht eine Serialisierung des ganzen Modells: ein
     * Chip traegt das Navigationsziel, an dem der Twin sein Subjekt neu
     * verankert, dessen qualifizierter Name nie gerendert wird, und eine
     * Zusicherung darueber wuerde die Chips zwingen, die Identitaet abzulegen,
     * die sie anklickbar macht.
     */
    const text = visibleTextOf(model);

    it('ist Prosa', () => {
        expect(model.mode).toBe('prose');
        expect(model.paragraphs.length).toBeGreaterThan(0);
        expect(model.sections).toHaveLength(0);
        expect(model.facts).toHaveLength(0);
    });

    it('zeigt nirgends einen qualifizierten Namen', () => {
        expect(QUALIFIED_NAME).toMatch(QUALIFIED_SHAPE);
        expect(text).not.toMatch(QUALIFIED_SHAPE);
        expect(text).not.toContain(QUALIFIED_NAME);
    });

    it('zeigt keine Konfidenz-Zahl', () => {
        expect(text).not.toContain('confidence');
        expect(text).not.toContain('0.9');
    });

    it('nennt Aufgerufenes als blosse Namen, bei fuenf gedeckelt, und sagt, wie viele fehlen', () => {
        expect(model.chips).toHaveLength(5);
        for (const chip of model.chips) {
            expect(chip.label).not.toContain('.');
            expect(chip.target).toBeDefined();
        }
        expect(model.chips.map((chip) => chip.label)).toEqual([
            'validateUser',
            'ValidationError',
            'UserEntity',
            'listUsers',
            'insert',
        ]);
        expect(text).toContain('1 further part is not listed here');
    });

    it('behaelt das Ziel, das einen Chip folgbar macht, obwohl es nie gezeigt wird', () => {
        expect(model.chips[0].target?.qualifiedName).toMatch(QUALIFIED_SHAPE);
    });

    it('sagt laut, dass der Absatz erzeugt wurde', () => {
        expect(text).toContain('Nobody wrote that description');
    });

    /*
     * W7a (Nutzerauftrag 2026-08-29): die beiden Linsen sagen nicht mehr, dass
     * sie "noch nicht gebaut" seien, sondern wo die Antwort in DIESEM Produkt
     * wirklich steht. Der Twin traegt sie nicht; der BUG-Assistent, der Flow und
     * die Aenderungsansicht tun es.
     */
    it('sagt von beiden Linsen, welche Flaeche sie stattdessen beantwortet', () => {
        expect(text).toContain('not to the twin');
        expect(text).toContain('ingest_traces');
        expect(text).toContain('The twin carries no version history');
        expect(text).toContain('[c]hange scope');
    });
});

describe('die Junior-Stufe fuehrt mit der Reihenfolge', () => {
    const model = buildTwinViewModel(CREATE_USER_IR, presentation(1, CORE_FACETS, 'plain'));

    it('ist ein kurzer Absatz plus die Schritte in ihrer Ordnung', () => {
        expect(model.mode).toBe('guided');
        expect(model.paragraphs).toHaveLength(2);
        expect(model.sections).toHaveLength(0);
        expect(model.steps).toHaveLength(CREATE_USER_IR.steps.value.length);
        expect(model.steps.map((step) => step.order)).toEqual([1, 2, 3, 4, 5, 6]);
    });

    it('setzt zwischen die Schritte ein Wort ueber die Reihenfolge, nie ueber die Absicht', () => {
        const texts = model.steps.map((step) => step.text);
        expect(texts[0].startsWith('First it hands work to validateUser')).toBe(true);
        expect(texts[texts.length - 1].startsWith('And last')).toBe(true);
        // Kein Bindeglied darf eine Absicht behaupten: der Index kennt die
        // Reihenfolge und niemand kennt den Grund.
        for (const text of texts) {
            expect(text).not.toMatch(/\bbecause\b|\bso that\b|\bin order to\b/);
        }
    });

    it('nennt den Ort, an den ein Schritt fuehrt, und laesst ihn anspringen', () => {
        expect(model.steps[0].text).toContain('validate.ts:');
        expect(model.steps[0].target).toBeDefined();
        expect(model.steps[0].factPath).toBe('steps[0]');
    });

    it('erklaert ein Wort beim ersten Auftreten, und nur die benutzten', () => {
        const words = model.terms.map((term) => term.term);
        expect(words).toContain('call site');
        expect(words).toContain('caller');
        expect(words).toContain('raise');
        // Die Fixture ist kein Einstiegspunkt (complexity ist unsupported), also
        // darf das Wort auch nicht erklaert werden.
        expect(words).not.toContain('entry point');
        expect(new Set(words).size).toBe(words.length);
        for (const term of model.terms) {
            expect(term.explanation.length).toBeGreaterThan(20);
        }
    });

    it('sagt es, wenn es gar keine Reihenfolge gibt, statt die Liste wegzulassen', () => {
        const leaf = { ...CREATE_USER_IR, steps: { ...CREATE_USER_IR.steps, value: [] } };
        const model = buildTwinViewModel(leaf, presentation(1, CORE_FACETS, 'plain'));
        expect(model.steps).toHaveLength(0);
        expect(visibleTextOf(model)).toContain('records no call order to walk through');
    });

    it('beschreibt ein Blatt nur als fehlenden aufgeloesten Aufruf, nicht als vollstaendiges Verhalten', () => {
        const leaf = { ...CREATE_USER_IR, steps: { ...CREATE_USER_IR.steps, value: [] } };
        const prose = buildTwinViewModel(leaf, presentation(0, CORE_FACETS, 'plain'));
        const guided = buildTwinViewModel(leaf, presentation(1, CORE_FACETS, 'plain'));

        expect(prose.subject).toMatch(/^createUser is a .+ \d+ lines in .+\.ts\./i);
        expect(prose.subject).toContain('The index resolves no outgoing calls');
        expect(prose.subject).not.toMatch(/works alone|complete recorded behavior/i);
        expect(visibleTextOf(guided)).toContain('The index resolves no outgoing calls');
        expect(visibleTextOf(guided)).not.toMatch(/does its work by itself|works alone|no shared state/i);
    });
});

describe('die Breite-zuerst-Liste, die keine Stufe mehr fuehrt', () => {
    it('nimmt eine Zeile je Familie, bevor eine Familie eine zweite bekommt', () => {
        const sections = visibleSections(CREATE_USER_IR, presentation(2, CORE_FACETS, 'plain'));
        const facts = topFacts(sections, 5);
        expect(facts.map((fact) => fact.label)).toEqual([
            'What it does, in order',
            'Who uses it',
            'What it reads',
            'How it can fail',
            'What checks it',
        ]);
        for (const fact of facts) {
            expect(fact.name).not.toContain('.');
            expect(fact.target).toBeDefined();
            expect(fact.factPath.length).toBeGreaterThan(0);
        }
    });
});

describe('die technische Tiefe sind die aufgezeichneten Fakten, unveraendert', () => {
    const model = buildTwinViewModel(CREATE_USER_IR, presentation(2, CORE_FACETS));

    it('rendert Sektionen und keine Prosa', () => {
        expect(model.mode).toBe('sections');
        expect(model.paragraphs).toHaveLength(0);
        expect(sectionNames(model)).toEqual([
            'purpose',
            'steps',
            'callers',
            'state',
            'errors',
            'effects',
            'tests',
        ]);
    });

    it('zeigt Datei und Zeile, die die Erzaehl-Tiefe weglaesst', () => {
        expect(visibleTextOf(model)).toContain('validate.ts:19');
    });

    it('haengt weder Konfidenz noch Zaehlung an', () => {
        const text = visibleTextOf(model);
        expect(text).not.toContain('confidence');
        expect(text).not.toContain('entries');
    });
});

describe('die Senior-Stufe fragt, was es kostet', () => {
    const model = buildTwinViewModel(CREATE_USER_IR, presentation(3, CORE_FACETS));
    const block = (name: string) => model.blocks.find((entry) => entry.name === name);

    it('zeichnet vier Kostenbloecke und keine Sektionen', () => {
        expect(model.mode).toBe('cost');
        expect(model.sections).toHaveLength(0);
        expect(model.blocks.map((entry) => entry.name))
            .toEqual(['fails', 'unchecked', 'depends', 'moves']);
    });

    it('fuehrt jeden Block mit dem Satz, der die Frage stellt', () => {
        for (const entry of model.blocks) {
            expect(entry.lead.length).toBeGreaterThan(20);
            expect(entry.emptyText.length).toBeGreaterThan(20);
        }
    });

    it('zieht die Fehlerpfade aus derselben Sektion, die die Medior-Stufe zeigt', () => {
        expect(block('fails')?.rows.map((row) => row.label)).toEqual(['ValidationError']);
    });

    it('zaehlt zusammen, was eine Aenderung mitbewegt', () => {
        const moves = block('moves');
        // Schritte, Umgebungslesungen und Effekte, nach Familie gruppiert.
        expect(moves?.rows.length).toBe(8);
        expect(new Set(moves?.rows.map((row) => row.group)).size).toBeGreaterThan(1);
        expect(moves?.weight).toBe('8 places to check');
    });

    it('zeigt keine Konfidenz: die Frage ist der Preis und nicht die Herkunft', () => {
        expect(visibleTextOf(model)).not.toContain('confidence');
    });

    it('sagt es in seiner eigenen Sprache, wo ein Block nichts zu bieten hat', () => {
        const calm = {
            ...CREATE_USER_IR,
            throws: { ...CREATE_USER_IR.throws, value: [] },
        };
        const empty = buildTwinViewModel(calm, presentation(3, CORE_FACETS));
        expect(empty.blocks.find((entry) => entry.name === 'fails')?.rows).toHaveLength(0);
        expect(visibleTextOf(empty)).toContain('No error path was recorded here');
    });

    it('nimmt einen Block mit seiner Linse weg', () => {
        const noErrors = buildTwinViewModel(CREATE_USER_IR, presentation(3, NARROW_FACETS));
        expect(noErrors.blocks.map((entry) => entry.name)).not.toContain('fails');
    });
});

describe('die Architekten-Stufe fragt, worauf es sitzt', () => {
    const model = buildTwinViewModel(CREATE_USER_IR, presentation(4, CORE_FACETS));
    const text = visibleTextOf(model);
    const block = (name: string) => model.blocks.find((entry) => entry.name === name);

    it('zeichnet Grund, Traeger und Schulden statt Sektionen', () => {
        expect(model.mode).toBe('ground');
        expect(model.sections).toHaveLength(0);
        expect(model.blocks.map((entry) => entry.name))
            .toEqual(['sits-on', 'carried-by', 'debts']);
    });

    it('fasst die Aufrufe zu Modulen zusammen und sagt, wie oft jedes getroffen wird', () => {
        const rows = block('sits-on')?.rows ?? [];
        expect(rows.map((row) => row.label)).toEqual([
            'util/validate.ts',
            'src/types.ts',
            'services/userService.ts',
            'repo/db.ts',
        ]);
        expect(rows[0].extras).toContain('2 call sites');
    });

    it('zeigt die Konfidenz, die der Provider aufgezeichnet hat', () => {
        expect(text).toContain('confidence 0.90');
        expect(block('sits-on')?.rows[0].confidenceLabel).toBe('confidence 0.90');
        // Ein Modul, in das nur unbewertete Aufrufe fuehren, sagt, dass es
        // nichts zu bewerten gab, statt eine Luecke zu lassen.
        expect(block('sits-on')?.rows[3].confidenceLabel).toBe('exact');
    });

    it('haelt die Grenzen des Index als eigenen Block, nicht als Fussnote', () => {
        expect(model.limits.length).toBeGreaterThan(4);
        const kinds = new Set(model.limits.map((limit) => limit.kind));
        expect(kinds).toContain('state');
        expect(kinds).toContain('engine');
        expect(kinds).toContain('generation');
        expect(text).toContain('records no test relation for TypeScript');
        expect(text).toContain('index generation 2');
    });

    it('sagt es, wo keine Regel gefeuert hat, statt einen sauberen Befund zu behaupten', () => {
        expect(block('debts')?.rows).toHaveLength(0);
        expect(text).toContain('That is not a clean bill');
    });

    it('behauptet keine Struktur-Kennzahl, die dieser Index gar nicht misst', () => {
        // complexity ist in dieser Fixture `unsupported`, und die Nullen darin
        // sind Platzhalter und keine Messwerte.
        expect(CREATE_USER_IR.complexity.state).toBe('unsupported');
        expect(text).not.toContain('branches through the body');
        expect(text).not.toContain('Loops nested');
    });
});

describe('die drei semantischen Bloecke', () => {
    const model = buildTwinViewModel(CREATE_USER_IR, presentation(2, CORE_FACETS));
    const block = (name: string) => model.sections.find((section) => section.name === name);

    it('gruppiert, was das Symbol haelt, wie es scheitert und was es beruehrt', () => {
        expect(block('state')?.block).toBe('data');
        expect(block('errors')?.block).toBe('errors');
        expect(block('effects')?.block).toBe('effects');
        // Alles andere handelt vom Symbol selbst und nicht davon, was es
        // erreicht, und wird darum absichtlich in keinen der drei gezwungen.
        expect(block('purpose')?.block).toBeUndefined();
        expect(block('steps')?.block).toBeUndefined();
        expect(block('callers')?.block).toBeUndefined();
        expect(block('tests')?.block).toBeUndefined();
    });

    it('zeichnet den Datenblock als Menge von Chips, mit allen Zielen und Belegen', () => {
        const data = block('state');
        expect(data?.rows.length).toBeGreaterThan(0);
        for (const row of data?.rows ?? []) {
            expect(row.display).toBe('chip');
            expect(row.factPath.length).toBeGreaterThan(0);
            expect(row.detail).toBeDefined();
        }
        expect(data?.rows.map((row) => row.label)).toContain('DB_URL');
        expect(data?.rows.map((row) => row.label)).toContain('User');
    });

    it('sagt nie, wo ein Fehler behandelt wird, weil der Index es nicht sieht', () => {
        const errors = block('errors');
        expect(errors?.rows.map((row) => row.label)).toEqual(['ValidationError']);
        expect(errors?.note).toContain('not visible to the index');
        const text = visibleTextOf(model);
        expect(text).not.toMatch(/handled at/i);
        expect(text).not.toMatch(/caught (?:at|in|by)/i);
    });

    it('sagt nichts ueber Behandlung, wenn nichts geworfen wird', () => {
        const noThrows = {
            ...CREATE_USER_IR,
            throws: { ...CREATE_USER_IR.throws, value: [], evidence: [] },
        };
        const errors = buildTwinViewModel(noThrows, presentation(2, CORE_FACETS)).sections.find(
            (section) => section.name === 'errors',
        );
        expect(errors?.rows).toHaveLength(0);
        expect(errors?.note).toBeUndefined();
    });

    it('nennt, was im Effekt-Block staende, statt sich als "keine Effekte" zu lesen', () => {
        const effects = block('effects');
        expect(effects?.rows).toHaveLength(0);
        expect(effects?.state).toBe('unsupported');
        // Keine Belege, weil CodeAtlas hier nichts behauptet hat.
        expect(effects?.factPath).toBe('');
        for (const kind of ['routes', 'outbound calls', 'writes']) {
            expect(effects?.emptyText).toContain(kind);
        }
    });

    it('rendert Effekte, die die Engine doch aufgezeichnet hat, nach Grenze gruppiert', () => {
        const withEffects = {
            ...CREATE_USER_IR,
            externalEffects: {
                state: 'known' as const,
                value: [
                    { kind: 'exposes-route' as const, detail: 'POST /users' },
                    { kind: 'io-write' as const, detail: 'users table' },
                ],
                evidence: [],
            },
        };
        const effects = buildTwinViewModel(withEffects, presentation(2, CORE_FACETS)).sections.find(
            (section) => section.name === 'effects',
        );
        expect(effects?.populated).toBe(true);
        expect(effects?.rows.map((row) => row.group)).toEqual(['Exposes routes', 'Writes']);
        expect(effects?.rows.map((row) => row.label)).toEqual(['POST /users', 'users table']);
        expect(effects?.factPath).toBe('externalEffects');
    });

    it('haelt den Datenblock als Daten lesbar, auch wenn writes nicht aufgezeichnet werden', () => {
        // Die 0.9.0-Engine kennt keine write-Relation, also ist die Familie leer
        // und unsupported. Wuerde das den Marker der Ueberschrift bestimmen,
        // staende "der Index kann das nicht beantworten" ueber einem
        // Umgebungswert, den er sehr wohl beantwortet hat.
        expect(CREATE_USER_IR.writes.state).toBe('unsupported');
        expect(block('state')?.state).toBe('known');
    });
});

describe('Facetten subtrahieren', () => {
    it('entfernt eine Sektion ganz, wenn ihre Linse aus ist', () => {
        const on = buildTwinViewModel(CREATE_USER_IR, presentation(2, CORE_FACETS));
        const off = buildTwinViewModel(CREATE_USER_IR, presentation(2, NARROW_FACETS));
        expect(sectionNames(on)).toContain('errors');
        expect(sectionNames(on)).toContain('state');
        expect(sectionNames(off)).not.toContain('errors');
        expect(sectionNames(off)).not.toContain('state');
        // Die Ueberschriften und ihre Inhalte sind weg. `ValidationError` selbst
        // steht weiter da, weil es auch eine Klasse ist, die dieses Symbol
        // konstruiert, und Errors auszuschalten ist keine Behauptung, die
        // Konstruktion habe nicht stattgefunden.
        expect(visibleTextOf(off)).not.toContain('Error paths');
        expect(visibleTextOf(off)).not.toContain('State and config');
        expect(visibleTextOf(off)).not.toContain('DB_URL');
    });

    it('behaelt die Aufrufliste, solange eine der beiden Linsen an ist, die sie besitzen', () => {
        const logicOnly = buildTwinViewModel(CREATE_USER_IR, presentation(2, [Facet.Logic]));
        const callsOnly = buildTwinViewModel(CREATE_USER_IR, presentation(2, [Facet.Calls]));
        expect(sectionNames(logicOnly)).toContain('steps');
        expect(sectionNames(callsOnly)).toContain('steps');
        expect(sectionNames(callsOnly)).not.toContain('purpose');
    });

    it('rendert Ueberschrift und ehrlichen Satz fuer eine Linse, die der Twin nicht beantwortet', () => {
        const model = buildTwinViewModel(CREATE_USER_IR, presentation(2, ALL_FACETS));
        const runtime = model.sections.find((section) => section.name === 'runtime');
        expect(runtime?.rows[0].label).toContain('not to the twin');
        // Kein Beleg anzubieten: CodeAtlas hat hier nichts ueber den Code behauptet.
        expect(runtime?.factPath).toBe('');
        expect(runtime?.rows[0].factPath).toBe('');
    });

    it('laesst eine ausgeschaltete Linse weg, statt sie zu erklaeren', () => {
        const model = buildTwinViewModel(CREATE_USER_IR, presentation(2, CORE_FACETS));
        expect(sectionNames(model)).not.toContain('runtime');
        expect(sectionNames(model)).not.toContain('changes');
    });

    it('laesst die Zahlen weg, die eine Linse aus dem Erzaehl-Absatz entfernt hat', () => {
        const withData = visibleTextOf(buildTwinViewModel(CREATE_USER_IR, presentation(0, CORE_FACETS)));
        const withoutData = visibleTextOf(
            buildTwinViewModel(CREATE_USER_IR, presentation(0, NARROW_FACETS)),
        );
        expect(withData).toContain('reads 1 value from the environment it runs in');
        expect(withoutData).not.toContain('from the environment it runs in');
    });
});

describe('Terminologie benennt die Sektionen um und sonst nichts', () => {
    const technical = buildTwinViewModel(CREATE_USER_IR, presentation(2, CORE_FACETS, 'technical'));
    const plain = buildTwinViewModel(CREATE_USER_IR, presentation(2, CORE_FACETS, 'plain'));

    it('tauscht die Ueberschriften', () => {
        expect(technical.sections.map((section) => section.title)).toEqual([
            'Purpose',
            'Steps',
            'Called by',
            'State and config',
            'Error paths',
            'Effects',
            'Tests',
        ]);
        expect(plain.sections.map((section) => section.title)).toEqual([
            'What this is for',
            'What it does, in order',
            'Who uses it',
            'What it reads',
            'How it can fail',
            'What it touches outside',
            'What checks it',
        ]);
    });

    it('laesst die Zeilen identisch', () => {
        expect(plain.sections.map((section) => section.rows.map((row) => row.label))).toEqual(
            technical.sections.map((section) => section.rows.map((row) => row.label)),
        );
    });
});

/*
 * Die Imports-Gruppe im Twin: der zweite Halbsatz des DATA-Blocks.
 *
 * Auch diese Faelle haben im Referenzprojekt keinen Unit-Test (dort prueft die
 * Suite die Gruppe und laesst das Widget sie zeichnen). Geprueft wird genau
 * das, was `withImportsSection` entscheidet: welche Linse sie einblendet, in
 * welchen Tiefen sie ueberhaupt gezeichnet wird, wo sie steht, und dass sie an
 * den Zeilen der IR nichts aendert.
 */
describe('withImportsSection', () => {
    const group = buildImportsGroup({
        imports: {
            entries: [
                { name: 'insert', module: '../repo/db', line: 4, origin: 'source', evidence: [] },
                { name: 'query', module: '../repo/db', line: 4, origin: 'source', evidence: [] },
            ],
            truncated: false,
            indexedTargets: ['src/repo/db.ts'],
            sourceRead: true,
            fileIrs: [CREATE_USER_IR],
        },
        irs: [CREATE_USER_IR],
        uri: CREATE_USER_IR.symbol.uri,
    });
    const withGroup = (depth: DepthLevel, facets: readonly Facet[] = CORE_FACETS) =>
        withImportsSection(
            buildTwinViewModel(CREATE_USER_IR, presentation(depth, facets)),
            group,
            presentation(depth, facets),
        );

    it('stellt die Gruppe neben die State-Sektion, nie an ihre Stelle', () => {
        const names = sectionNames(withGroup(2));
        expect(names).toContain('state');
        expect(names[names.indexOf('state') + 1]).toBe('imports');
    });

    it('zeigt sie unter der Data-Linse und nimmt sie mit ihr weg', () => {
        expect(sectionNames(withGroup(2))).toContain('imports');
        expect(sectionNames(withGroup(2, NARROW_FACETS))).not.toContain('imports');
    });

    it('zeichnet sie nur dort, wo eine Namensliste eine Antwort ist', () => {
        expect(sectionNames(withGroup(0))).not.toContain('imports');
        expect(sectionNames(withGroup(1))).not.toContain('imports');
    });

    it('faltet sie fuer den Senior in das, was eine Aenderung mitbewegt', () => {
        const moves = withGroup(3).blocks.find((entry) => entry.name === 'moves');
        expect(moves?.rows.some((row) => row.id.startsWith('import-'))).toBe(true);
    });

    it('faltet sie fuer den Architekten in den Grund, auf dem das Symbol sitzt', () => {
        const ground = withGroup(4).blocks.find((entry) => entry.name === 'sits-on');
        expect(ground?.rows.some((row) => row.id.startsWith('import-'))).toBe(true);
        expect(ground?.weight).toBe('6 entries');
    });

    it('nennt die Sektion im Vokabular der Praesentation', () => {
        const plain = withImportsSection(
            buildTwinViewModel(CREATE_USER_IR, presentation(2, CORE_FACETS, 'plain')),
            group,
            presentation(2, CORE_FACETS, 'plain'),
        );
        expect(plain.sections.find((section) => section.name === 'imports')?.title)
            .toBe('What the file brings in');
    });

    it('laesst das Modell unberuehrt, solange keine Gruppe da ist', () => {
        const model = buildTwinViewModel(CREATE_USER_IR, presentation(2, CORE_FACETS));
        expect(withImportsSection(model, undefined, presentation(2, CORE_FACETS))).toBe(model);
    });

    it('bringt den Wortlaut der Gruppe bis in den sichtbaren Text', () => {
        expect(visibleTextOf(withGroup(2))).toContain('1 used in this file');
    });
});
