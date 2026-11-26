import { describe, expect, it } from 'vitest';

import { citationComplianceOf } from './eval-citation-summary.mjs';

describe('citationComplianceOf', () => {
    it('zaehlt nur gemessene Zitatpruefungen in Treffer und Nenner', () => {
        expect(citationComplianceOf([
            { check: { measured: true, ok: true } },
            { check: { measured: true, ok: false } },
            { check: { measured: false, ok: false } },
        ])).toEqual({
            citationCompliance: 0.5,
            citationMeasured: 2,
            citationUnmeasured: 1,
        });
    });

    it('wertet eine fehlende Pruefung als unmessbar statt als Zitatfehler', () => {
        expect(citationComplianceOf([
            { check: { measured: false, ok: false } },
            {},
        ])).toEqual({
            citationCompliance: 0,
            citationMeasured: 0,
            citationUnmeasured: 2,
        });
    });
});
