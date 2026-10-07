import { describe, expect, it } from 'vitest';
import { Color } from 'three';
import { edgeArrowGeometry, edgePulseGeometry, type EdgePulsePath } from './EdgePulseLayer';

const path = (id: string, type = 'CALLS'): EdgePulsePath => ({ id, type, points: [{ x: 4, y: 0, z: 0 }, { x: 3, y: 0, z: 0 }, { x: 3, y: 0, z: -3 }] });
describe('batched direction pulse geometry', () => {
    it('measures progress along source-to-target distance even when coordinates decrease', () => {
        const geometry = edgePulseGeometry([path('a')]);
        const positions = geometry.getAttribute('position'), flow = geometry.getAttribute('edgeFlow');
        expect(positions.count).toBe(4);
        expect([flow.getX(0), flow.getX(1), flow.getX(2), flow.getX(3)]).toEqual([0, .25, .25, 1]);
        expect([positions.getX(0), positions.getZ(3)]).toEqual([4, -3]);
        expect(flow.getY(0)).toBe(flow.getY(3));
        geometry.dispose();
    });
    it('keeps separate curves disconnected in one batch and preserves focus intensity', () => {
        const geometry = edgePulseGeometry([path('a'), { ...path('b'), opacity: .08 }]);
        const flow = geometry.getAttribute('edgeFlow');
        expect(flow.count).toBe(8);
        expect(flow.getX(3)).toBe(1); expect(flow.getX(4)).toBe(0);
        expect(flow.getY(0)).not.toBe(flow.getY(4));
        expect(flow.getZ(4)).toBeCloseTo(.08);
        geometry.dispose();
    });
    it('omits symmetric, unknown, invisible and invalid paths without fabricating a direction', () => {
        const geometry = edgePulseGeometry([path('s', 'SIMILAR_TO'), path('u', 'NEW_TYPE'), { ...path('hidden'), opacity: 0 },
            { ...path('zero'), points: [{ x: 0, y: 0, z: 0 }, { x: 0, y: 0, z: 0 }] },
            { ...path('bad'), points: [{ x: NaN, y: 0, z: 0 }, { x: 0, y: 0, z: 0 }] },
            { ...path('mixed', 'RELATIONSHIPS'), types: ['CALLS', 'IMPORTS'] }]);
        expect(geometry.getAttribute('position').count).toBe(4);
        expect([...geometry.getAttribute('position').array].every(Number.isFinite)).toBe(true);
        geometry.dispose();
    });
    it('draws a pulse and its arrow in the colour the view chose for the line, else in the hue of its type', () => {
        const chosen = new Color('#62d2a2'), calls = new Color('#579fc7');
        for (const build of [edgePulseGeometry, edgeArrowGeometry]) {
            const geometry = build([{ ...path('journey'), color: '#62d2a2' }, path('plain')]);
            const colors = geometry.getAttribute('color'), last = colors.count - 1;
            expect([colors.getX(0), colors.getY(0), colors.getZ(0)].map(value => Number(value.toFixed(4)))).toEqual([chosen.r, chosen.g, chosen.b].map(value => Number(value.toFixed(4))));
            expect([colors.getX(last), colors.getY(last), colors.getZ(last)].map(value => Number(value.toFixed(4)))).toEqual([calls.r, calls.g, calls.b].map(value => Number(value.toFixed(4))));
            geometry.dispose();
        }
    });
});
