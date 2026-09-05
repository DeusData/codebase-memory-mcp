/**
 * Die Grenzen als Regel, an erfundenen Ergebnissen bewiesen.
 *
 * Ein Trockentest und kein Modellauf, und das ist hier keine Bequemlichkeit:
 * geprueft wird die ENTSCHEIDUNG, nicht die Messung. Ein Test, der sechs
 * Modelle laden muesste, um zu zeigen, dass 0.55 unter 0.6 liegt, waere ein
 * Test, den niemand bei jedem `npm run test:unit` fahren wuerde, und genau
 * dieser Test muss bei jedem Lauf mitkommen: er ist die Absicherung dagegen,
 * dass die Grenzen wieder zu einem Satz im ADR werden.
 */

import { describe, expect, it } from 'vitest';

import { EVAL_BOUNDS, boundViolations, violationsOf } from './eval-bounds.mjs';

const good = { name: 'Qwen3.5-2B', passRate: 0.682, citationCompliance: 0.932 };
const gemma = { name: 'gemma-4-E4B', passRate: 0.841, citationCompliance: 1 };

describe('EVAL_BOUNDS', () => {
    it('nennt die zwei Zahlen des ADR und wo sie herkommen', () => {
        expect(EVAL_BOUNDS.passRate).toBe(0.6);
        expect(EVAL_BOUNDS.citationCompliance).toBe(0.9);
        expect(EVAL_BOUNDS.source).toMatch(/w5b\.test\.mjs/);
    });
});

describe('violationsOf', () => {
    it('schweigt zu einem Ergebnis, das beide Grenzen haelt', () => {
        expect(violationsOf('Klasse A', good)).toEqual([]);
    });

    it('meldet eine zu niedrige Trefferquote mit Zahl und Grenze', () => {
        const found = violationsOf('Klasse A', { ...good, passRate: 0.59 });
        expect(found).toHaveLength(1);
        expect(found[0]).toMatch(/passRate 0\.59/);
        expect(found[0]).toMatch(/0\.6/);
    });

    it('meldet eine zu niedrige Zitattreue getrennt von der Trefferquote', () => {
        const found = violationsOf('Klasse A', { ...good, citationCompliance: 0.899 });
        expect(found).toHaveLength(1);
        expect(found[0]).toMatch(/Zitattreue/);
    });

    it('laesst die Grenze selbst gelten und nicht erst den Wert darueber', () => {
        expect(violationsOf('x', { passRate: 0.6, citationCompliance: 0.9 })).toEqual([]);
    });

    it('haelt eine fehlende Zahl fuer eine Verletzung und nicht fuer unbekannt', () => {
        expect(violationsOf('x', {})).toHaveLength(2);
        expect(violationsOf('x', { passRate: 0.7 })[0]).toMatch(/nicht gemessen/);
    });

    it('meldet beide Grenzen, wenn ein Modell beide reisst', () => {
        expect(violationsOf('x', { passRate: 0.2, citationCompliance: 0.3 })).toHaveLength(2);
    });
});

describe('boundViolations', () => {
    it('laesst den aufgezeichneten Lauf durch, beide Sieger', () => {
        expect(boundViolations({ A: good, B: gemma })).toEqual([]);
    });

    it('haelt eine Klasse ohne Sieger fuer eine Verletzung', () => {
        const found = boundViolations({ A: good, B: undefined });
        expect(found).toHaveLength(1);
        expect(found[0]).toMatch(/kein Sieger/);
    });

    it('nennt die Klasse und das Modell, damit man weiss, wen es betrifft', () => {
        const found = boundViolations({ A: { name: 'LFM2.5-1.2B', passRate: 0.295, citationCompliance: 0.432 } });
        expect(found).toHaveLength(2);
        for (const line of found) {
            expect(line).toMatch(/Klasse A \(LFM2\.5-1\.2B\)/);
        }
    });
});
