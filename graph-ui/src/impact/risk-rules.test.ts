/*
 * Herkunft: portiert am 2026-08-28 aus CodeAtlasIDE,
 * /Users/bernhard/Desktop/CodeAtlasIDE/theia-extensions/codeatlas-views/test/risk-rules.test.ts
 * Gleicher Urheber (Bernhard Jackiewicz). Vollstaendig uebernommen: jeder Fall
 * dieser Tabelle steht hier so, wie er dort steht. Einzige Aenderung ist der
 * Importpfad auf die portierte Datei im selben Verzeichnis. Der Grund, die
 * Tabelle ganz zu uebernehmen statt eine eigene zu schreiben: die Schwellen
 * sind die Aussage, und zwei Tabellen ueber dieselben Schwellen waeren zwei
 * Wahrheiten, sobald jemand eine davon anfasst.
 */
import { describe, expect, it } from 'vitest';
import {
    RISK_THRESHOLDS,
    completenessOf,
    hasComplexityReadings,
    overallRisk,
    perSymbolRisk,
    worseOf,
} from './risk-rules';
import type { RiskLevel, SymbolRiskInput } from './risk-rules';

/**
 * The rules are a table, so they are tested as one.
 *
 * Every case below sits on a boundary rather than in the middle of a band: one
 * below the threshold, on it, and where the bands meet. A rule that is right in
 * the middle of its range and wrong at its edge is the rule that produces a
 * MEDIUM where a reader expected a HIGH, which is the only kind of mistake this
 * file can make that anybody notices.
 */
describe('perSymbolRisk', () => {

    const cases: [string, SymbolRiskInput, RiskLevel][] = [
        ['nothing measured at all is low, never high', {}, 'low'],
        ['every reading at zero is low', {
            isEntryPoint: false,
            transitiveLoopDepth: 0,
            unguardedRecursion: false,
            fanIn: 0,
            cyclomatic: 0,
            cognitive: 0,
            allocInLoop: false,
            linearScanInLoop: false,
        }, 'low'],
        ['an entry point is high on its own', { isEntryPoint: true }, 'high'],
        ['an entry point outranks every quiet reading', {
            isEntryPoint: true, cyclomatic: 1, cognitive: 1, fanIn: 0,
        }, 'high'],
        ['loop nesting one below the bound is not high', {
            transitiveLoopDepth: RISK_THRESHOLDS.loopDepthHigh - 1,
        }, 'low'],
        ['loop nesting on the bound is high', {
            transitiveLoopDepth: RISK_THRESHOLDS.loopDepthHigh,
        }, 'high'],
        ['unguarded recursion is high on its own', { unguardedRecursion: true }, 'high'],
        ['fan-in one below the high bound is medium, not high', {
            fanIn: RISK_THRESHOLDS.fanInHigh - 1,
        }, 'medium'],
        ['fan-in on the high bound is high', { fanIn: RISK_THRESHOLDS.fanInHigh }, 'high'],
        ['fan-in one below the medium band is low', {
            fanIn: RISK_THRESHOLDS.fanInMediumMin - 1,
        }, 'low'],
        ['fan-in at the bottom of the medium band is medium', {
            fanIn: RISK_THRESHOLDS.fanInMediumMin,
        }, 'medium'],
        ['cyclomatic one below the bound is low', {
            cyclomatic: RISK_THRESHOLDS.cyclomaticMedium - 1,
        }, 'low'],
        ['cyclomatic on the bound is medium', {
            cyclomatic: RISK_THRESHOLDS.cyclomaticMedium,
        }, 'medium'],
        ['cognitive one below the bound is low', {
            cognitive: RISK_THRESHOLDS.cognitiveMedium - 1,
        }, 'low'],
        ['cognitive on the bound is medium', {
            cognitive: RISK_THRESHOLDS.cognitiveMedium,
        }, 'medium'],
        ['allocation inside a loop is medium', { allocInLoop: true }, 'medium'],
        ['a linear scan inside a loop is medium', { linearScanInLoop: true }, 'medium'],
        ['a well tested symbol is not made safe by its tests', {
            testedByCount: 12, fanIn: RISK_THRESHOLDS.fanInHigh,
        }, 'high'],
        ['an untested symbol is not made risky by that alone', { testedByCount: 0 }, 'low'],
    ];

    for (const [name, input, expected] of cases) {
        it(name, () => {
            expect(perSymbolRisk(input)).toBe(expected);
        });
    }

    it('treats a missing number as missing rather than as zero at every boundary', () => {
        // The distinction that matters: absent must not reach a threshold, and
        // must not be reported as a measurement either.
        expect(perSymbolRisk({ transitiveLoopDepth: undefined })).toBe('low');
        expect(perSymbolRisk({ fanIn: undefined })).toBe('low');
        expect(hasComplexityReadings({ fanIn: 40 })).toBe(false);
        expect(hasComplexityReadings({ cyclomatic: 0 })).toBe(true);
    });

    it('ignores a reading that is not a finite number', () => {
        expect(perSymbolRisk({ fanIn: Number.NaN })).toBe('low');
        expect(perSymbolRisk({ cyclomatic: Number.POSITIVE_INFINITY })).toBe('low');
    });
});

describe('overallRisk', () => {

    const cases: [string, RiskLevel[], number, number, RiskLevel][] = [
        ['nothing affected at all is low', [], 0, 0, 'low'],
        ['every symbol low with no endpoint and no gap is low', ['low', 'low'], 0, 0, 'low'],
        ['one high symbol makes the change high', ['low', 'high'], 0, 0, 'high'],
        ['one medium symbol makes the change medium', ['low', 'medium'], 0, 0, 'medium'],
        ['one affected endpoint makes the change medium', ['low'], 1, 0, 'medium'],
        ['two affected endpoints are still medium', ['low'], 2, 0, 'medium'],
        ['three affected endpoints make the change high', ['low'], 3, 0, 'high'],
        ['one untested affected symbol makes the change medium', ['low'], 0, 1, 'medium'],
        ['four untested affected symbols are still medium', ['low'], 0, 4, 'medium'],
        ['five untested affected symbols make the change high', ['low'], 0, 5, 'high'],
        ['endpoints and gaps do not add up into a level neither reaches', ['low'], 2, 4, 'medium'],
        ['a high symbol outranks a clean endpoint and test picture', ['high'], 0, 0, 'high'],
    ];

    for (const [name, levels, endpoints, untested, expected] of cases) {
        it(name, () => {
            expect(overallRisk(levels, endpoints, untested)).toBe(expected);
        });
    }

    it('uses the exact plan thresholds and not a rounded version of them', () => {
        expect(RISK_THRESHOLDS).toEqual({
            loopDepthHigh: 3,
            fanInHigh: 10,
            fanInMediumMin: 4,
            fanInMediumMax: 9,
            cyclomaticMedium: 10,
            cognitiveMedium: 15,
            endpointsHigh: 3,
            endpointsMedium: 1,
            untestedHigh: 5,
            untestedMedium: 1,
        });
    });
});

describe('completeness', () => {

    it('counts the symbols whose level rests on absent readings', () => {
        const completeness = completenessOf([
            { cyclomatic: 4, cognitive: 2 },
            { fanIn: 12 },
            {},
        ]);
        expect(completeness).toEqual({ measured: 1, unmeasured: 2, total: 3 });
    });

    it('reports a fully unmeasured set rather than reporting nothing', () => {
        expect(completenessOf([{}, {}])).toEqual({ measured: 0, unmeasured: 2, total: 2 });
        expect(completenessOf([])).toEqual({ measured: 0, unmeasured: 0, total: 0 });
    });
});

describe('worseOf', () => {

    it('keeps the stronger claim', () => {
        expect(worseOf('low', 'medium')).toBe('medium');
        expect(worseOf('high', 'medium')).toBe('high');
        expect(worseOf('low', 'low')).toBe('low');
    });
});
