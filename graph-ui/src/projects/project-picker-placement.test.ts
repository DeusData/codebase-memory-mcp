import { describe, expect, it } from 'vitest';
import { projectPickerPlacement } from './project-picker-placement';

describe('project picker viewport bounds', () => {
    it('opens below a normal header with room for project results', () => {
        const box = projectPickerPlacement({ top: 18, bottom: 50, right: 1180 }, { width: 1200, height: 900 });
        expect(box.top).toBeGreaterThan(50);
        expect(box.maxHeight).toBeGreaterThanOrEqual(240);
        expect(box.left + box.width).toBeLessThanOrEqual(1188);
    });

    it.each([
        { width: 900, height: 400, top: 170, bottom: 202, right: 880 },
        { width: 320, height: 360, top: 210, bottom: 242, right: 310 },
        { width: 1400, height: 600, top: 520, bottom: 552, right: 1380 },
    ])('keeps results and footer inside a short or wrapped viewport ($width x $height)', (viewport) => {
        const box = projectPickerPlacement(viewport, viewport);
        expect(box.top).toBeGreaterThanOrEqual(12);
        expect(box.left).toBeGreaterThanOrEqual(12);
        expect(box.top + box.maxHeight).toBeLessThanOrEqual(viewport.height - 12);
        expect(box.left + box.width).toBeLessThanOrEqual(viewport.width - 12);
        expect(box.maxHeight).toBeGreaterThanOrEqual(240);
    });
});
