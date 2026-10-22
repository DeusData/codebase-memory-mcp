import { expect, it } from 'vitest';
import { edgeArrowGeometry, edgeArrowScreenScale } from './EdgePulseLayer';

it('keeps the complete arrow under 4.5 CSS pixels and smaller in embedded views', () => {
    const geometry = edgeArrowGeometry([{ id: 1, type: 'CALLS', points: [{ x: 0, y: 0, z: 0 }, { x: 1, y: 0, z: 0 }] }]);
    const positions = geometry.getAttribute('position');
    const length = positions.getX(0) - positions.getX(1);
    for (const [width, height] of [[712, 255], [1920, 1080], [7680, 4320]]) {
        expect(length * edgeArrowScreenScale(width!, height!)).toBeLessThanOrEqual(4.5);
    }
    expect(edgeArrowScreenScale(712, 255)).toBeLessThan(edgeArrowScreenScale(1920, 1080));
    expect(Number.isFinite(edgeArrowScreenScale(0, Number.NaN))).toBe(true);
    geometry.dispose();
});

it('keeps even emphasized arrows translucent while preserving relative emphasis and edge colors', () => {
    const points = [{ x: 0, y: 0, z: 0 }, { x: 1, y: 0, z: 0 }];
    const geometry = edgeArrowGeometry([{ id: 1, type: 'CALLS', points, opacity: 1 },
        { id: 2, type: 'CALLS', points, opacity: .5 }, { id: 3, type: 'IMPORTS', points, opacity: 1 }]);
    const flow = geometry.getAttribute('arrowFlow'), color = geometry.getAttribute('color');
    expect(flow.getW(0)).toBeCloseTo(.3);
    expect(flow.getW(3)).toBeCloseTo(flow.getW(0) / 2);
    expect([color.getX(0), color.getY(0), color.getZ(0)]).not.toEqual([color.getX(6), color.getY(6), color.getZ(6)]);
    geometry.dispose();
});

it('builds one arrow triangle per segment with semantic source-to-target progress', () => {
    const geometry = edgeArrowGeometry([{ id: 'a', type: 'CALLS', points: [{ x: 0, y: 0, z: 0 }, { x: 3, y: 0, z: 0 }, { x: 3, y: 4, z: 0 }] }]);
    expect(geometry.getAttribute('position').count).toBe(6);
    const flow = geometry.getAttribute('arrowFlow');
    expect(flow.getX(0)).toBe(0); expect(flow.getY(0)).toBeCloseTo(3 / 7);
    expect(flow.getX(3)).toBeCloseTo(3 / 7); expect(flow.getY(3)).toBe(1);
    expect(flow.getZ(0)).toBe(flow.getZ(3));
    expect(Array.from(geometry.getAttribute('arrowEnd').array).slice(0, 3)).toEqual([3, 0, 0]);
    geometry.dispose();
});
it('never invents arrow direction for symmetric or unknown edges and skips degenerate paths', () => {
    const points = [{ x: 0, y: 0, z: 0 }, { x: 1, y: 0, z: 0 }];
    const geometry = edgeArrowGeometry([{ id: 1, type: 'SIMILAR_TO', points }, { id: 2, type: 'UNKNOWN', points },
        { id: 3, type: 'CALLS', points: [points[0]!, points[0]!] }]);
    expect(geometry.getAttribute('position').count).toBe(0); geometry.dispose();
});
it('does not impose a hidden edge-count cap', () => {
    const geometry = edgeArrowGeometry(Array.from({ length: 3000 }, (_, id) => ({ id, type: 'CALLS',
        points: [{ x: id, y: 0, z: 0 }, { x: id + 1, y: 1, z: 0 }] })));
    expect(geometry.getAttribute('position').count).toBe(9000); geometry.dispose();
});
