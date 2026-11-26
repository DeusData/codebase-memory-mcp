/*
 * Die Tiefenleiter, ueber jedes aufgezeichnete Symbol gelegt statt ueber eins.
 *
 * Portiert aus CodeAtlasIDE,
 * theia-extensions/codeatlas-views/test/render-model.depths.test.ts, ohne die
 * vierzig Snapshots und mit denselben Invarianten daneben. Die Begruendung des
 * Originals gilt unveraendert:
 *
 * `render-model.test.ts` sagt den Vertrag: was jede Tiefe sagen darf, was eine
 * Facette subtrahiert, was die Erzaehl-Tiefe nie zeigen darf. Es sagt ihn gegen
 * `createUser`, und das ist die richtige Fixture dafuer, weil dort jede
 * Faktenfamilie gefuellt ist und jeder Zweig des Absatzbauers etwas zu tun hat.
 *
 * Das ist zugleich sein blinder Fleck. Ein Symbol, das niemand ruft; eins, das
 * nichts ruft; eins ohne Fehlerpfad; ein Knoten, dessen einzige interessante
 * Eigenschaft ist, dass drei Dienste ihn benutzen: jedes davon nimmt einen
 * anderen Weg durch dasselbe Render-Modell. Die Leer-Saetze, die Ein- und
 * Mehrzahl, das "not listed here" und die Risiko-Zeilen waeren sonst nur durch
 * Hinsehen gedeckt.
 *
 * Die Fixture-Menge wird aus dem Verzeichnis gelesen und nicht hier
 * aufgezaehlt: eine elfte Aufzeichnung soll das Netz weiten, ohne dass jemand
 * daran denken muss, hier nachzutragen.
 */

import { describe, expect, it } from 'vitest';

import {
    CORE_FACETS,
    QUALIFIED_SHAPE,
    RECORDED_IRS,
    presentation,
} from '../test-support/twin-fixtures';
import type { DepthLevel } from './presentation-profile';
import { buildTwinViewModel, visibleTextOf } from './render-model';

/** Die fuenf Leser und wie sich das Modell fuer jeden nennt. */
const MODE_BY_DEPTH: Readonly<Record<DepthLevel, string>> = {
    0: 'prose',
    1: 'guided',
    2: 'sections',
    3: 'cost',
    4: 'ground',
};

/** Die fuenf Rasten, in der Reihenfolge, in der der Regler sie durchlaeuft. */
const LEVELS = [0, 1, 2, 3, 4] as const;

describe('die aufgezeichnete Fixture-Menge', () => {
    it('haelt die zehn Symbole, ueber die das Netz gespannt ist', () => {
        expect(RECORDED_IRS.map((entry) => entry.id)).toEqual([
            'ir-createUser',
            'ir-getOrder',
            'ir-hotspotScan',
            'ir-insert',
            'ir-listUsers',
            'ir-orderService-create',
            'ir-query',
            'ir-userService-create',
            'ir-validateUser',
            'ir-walk',
        ]);
    });

    it('nennt keinen Pfad der Maschine, die sie aufgezeichnet hat', () => {
        for (const { id, ir } of RECORDED_IRS) {
            const body = JSON.stringify(ir);
            expect(body, `${id} verraet einen absoluten Pfad`).not.toContain('/Users/');
            expect(body, `${id} verraet einen Temp-Pfad`).not.toContain('/var/folders');
        }
    });

    it('deckt die Formen ab, die eine einzelne Fixture nicht hat', () => {
        const steps = RECORDED_IRS.map((entry) => entry.ir.steps.value.length);
        const callers = RECORDED_IRS.map((entry) => entry.ir.calledBy.value.length);
        const raises = RECORDED_IRS.map((entry) => entry.ir.throws.value.length);
        // Ein Blatt, das nichts ruft, und ein Symbol, das mehreres ruft.
        expect(Math.min(...steps)).toBe(0);
        expect(Math.max(...steps)).toBeGreaterThan(1);
        // Ein Symbol, das niemand ruft, und eins, das mehrere Aufrufer erreichen.
        expect(Math.min(...callers)).toBe(0);
        expect(Math.max(...callers)).toBeGreaterThan(1);
        // Ein Symbol ohne Fehlerpfad und eins mit.
        expect(Math.min(...raises)).toBe(0);
        expect(Math.max(...raises)).toBeGreaterThan(0);
    });
});

describe('was die Leiter garantiert, welches Symbol auch immer', () => {
    for (const { id, ir } of RECORDED_IRS) {
        it(`${id} nennt seine Tiefe an jeder Raste ehrlich`, () => {
            for (const depth of LEVELS) {
                const model = buildTwinViewModel(ir, presentation(depth, CORE_FACETS));
                expect(model.mode).toBe(MODE_BY_DEPTH[depth]);
                expect(model.depth).toBe(depth);
            }
        });

        it(`${id} zeigt an der Erzaehl-Tiefe keinen qualifizierten Namen`, () => {
            const model = buildTwinViewModel(ir, presentation(0, CORE_FACETS));
            const text = visibleTextOf(model);
            expect(ir.symbol.qualifiedName ?? '').toMatch(QUALIFIED_SHAPE);
            expect(text).not.toContain(ir.symbol.qualifiedName);
            expect(text).not.toMatch(QUALIFIED_SHAPE);
        });

        it(`${id} zeigt vor der Architekten-Stufe keine Konfidenz`, () => {
            for (const depth of [0, 1, 2, 3] as const) {
                const text = visibleTextOf(buildTwinViewModel(ir, presentation(depth, CORE_FACETS)));
                expect(text, `${id} an Tiefe ${depth}`).not.toContain('confidence');
            }
        });

        it(`${id} sagt an jeder Tiefe etwas, statt leer zu rendern`, () => {
            // Das eine, was ein Leser immer verlangen darf: das Panel ist nie
            // leer. Ein Symbol ohne Aufrufer, ohne Fehlerpfad und ohne eigene
            // Aufrufe ist genau die Stelle, an der ein Render-Modell still gar
            // nichts produziert, und vier der zehn haben diese Form.
            for (const depth of LEVELS) {
                const model = buildTwinViewModel(ir, presentation(depth, CORE_FACETS));
                const text = visibleTextOf(model);
                expect(text.trim().length, `${id} an Stufe ${depth} hat nichts gerendert`).toBeGreaterThan(0);
            }
        });

        it(`${id} sagt auf keinen zwei Stufen dasselbe`, () => {
            /*
             * Der Nutzerbefund von W13 lautete "ich seh gar keine
             * Aenderungen", und er ist an EINEM Symbol gemessen worden. Hier
             * steht er ueber allen zehn: ein Regler, der an irgendeinem Symbol
             * zweimal dasselbe ergibt, ist an genau diesem Symbol kaputt, und
             * ein Symbol ohne Aufrufer und ohne Fehlerpfad ist die Stelle, an
             * der zwei Stufen am ehesten zusammenfallen.
             */
            const texts = LEVELS.map((depth) =>
                visibleTextOf(buildTwinViewModel(ir, presentation(depth, CORE_FACETS))));
            expect(new Set(texts).size, `${id} hat zwei gleiche Stufen`).toBe(LEVELS.length);
        });

        it(`${id} gibt an der Erzaehl-Tiefe zu, dass der Absatz erzeugt wurde`, () => {
            // Die Erzaehl-Tiefe ist die eine Stelle, an der CodeAtlas Prosa
            // schreibt, und der Hinweis ist keine Dekoration: ein erzeugter
            // Satz, der nicht sagt, dass er erzeugt wurde, ist das Produkt, das
            // eine Urheberschaft behauptet, die es nicht hat. Er muss auch die
            // leeren Formen ueberleben, und genau dort ist ein Absatzbauer am
            // ehesten versucht auszusteigen.
            const text = visibleTextOf(buildTwinViewModel(ir, presentation(0, CORE_FACETS)));
            expect(text).toContain('Nobody wrote that description');
        });
    }
});
