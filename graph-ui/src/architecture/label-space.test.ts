import { describe, expect, it } from 'vitest';
import { journeyChipForm, labelEdges, labelInset, labelRect, labelsCollide, placeSceneLabels, placeSecondaryLabels, spotRect, type LabelBox, type SceneChip, type SecondaryLabel } from './label-space';

// The compact Overview and Endpoints label: padding 5px 7px and a 1px border (spatial-architecture.css).
const compact: LabelBox = { width: 100, height: 24, edgeX: 8, edgeY: 6 };
const style = (values: Record<string, string>) => ({ getPropertyValue: (name: string) => values[name] ?? '' });

describe('label space', () => {
    it('reads padding plus border on the thinner side of each axis', () => {
        expect(labelEdges(style({ 'padding-left': '7px', 'padding-right': '9px', 'border-left-width': '1px', 'border-right-width': '1px',
            'padding-top': '5px', 'padding-bottom': '5px', 'border-top-width': '1px', 'border-bottom-width': '0px' }))).toEqual([8, 5]);
        expect(labelEdges(style({}))).toEqual([0, 0]);
    });
    it('lets two neighbours share their padding but never reach into either text', () => {
        const inset = labelInset([compact, compact]);
        expect(inset).toEqual([4, 3]);
        const left = labelRect(100, 50, compact.width, compact.height, inset);
        // The boxes overlap by exactly one label's padding plus border: the neighbour's edge stops where the text begins.
        const touching = labelRect(100 + compact.width - compact.edgeX, 50, compact.width, compact.height, inset);
        expect(labelsCollide(left, touching)).toBe(false);
        expect(labelsCollide(left, labelRect(100 + compact.width - compact.edgeX - 1, 50, compact.width, compact.height, inset))).toBe(true);
        const below = labelRect(100, 50 + compact.height - compact.edgeY, compact.width, compact.height, inset);
        expect(labelsCollide(left, below)).toBe(false);
        expect(labelsCollide(left, labelRect(100, 50 + compact.height - compact.edgeY - 1, compact.width, compact.height, inset))).toBe(true);
    });
    it('takes the thinnest padding when labels differ and keeps a margin around an estimate', () => {
        expect(labelInset([compact, { width: 145, height: 30, edgeX: 13, edgeY: 8 }])).toEqual([4, 3]);
        expect(labelInset([])).toEqual([0, 0]);
        expect(labelRect(100, 50, 80, 38, [-5, 0])).toEqual({ left: 55, right: 145, top: 31, bottom: 69 });
    });
    it('moves a folder or lane name to the next free corner instead of under a chip, and hides it only when no corner is free', () => {
        // "Outsi ◉ (root)": the lane name at its first corner runs into the (root) chip.
        const chip = labelRect(160, 108, 70, 24, [0, 0]);
        const outside = { id: 'hierarchy:outside', width: 44, height: 14, spots: [{ x: 120, y: 100, align: 'start' as const }, { x: 120, y: 30, align: 'start' as const }] };
        expect(placeSecondaryLabels([outside], [chip], 400, 300)).toEqual(new Map([['hierarchy:outside', 1]]));
        expect(labelsCollide(spotRect(outside.spots[1]!, 44, 14), chip)).toBe(false);
        // Two names that want the same corner: the first keeps it, the second takes its other corner.
        const a = { id: 'a', width: 40, height: 14, spots: [{ x: 10, y: 10, align: 'start' as const }] };
        const b = { id: 'b', width: 40, height: 14, spots: [{ x: 20, y: 12, align: 'start' as const }, { x: 200, y: 12, align: 'end' as const }] };
        expect(placeSecondaryLabels([a, b], [], 400, 300)).toEqual(new Map([['a', 0], ['b', 1]]));
        expect(spotRect(b.spots[1]!, 40, 14)).toMatchObject({ left: 160, right: 200 });
        // Covered everywhere or outside the canvas: hidden, never stacked.
        const covered = { id: 'c', width: 40, height: 14, spots: [{ x: 10, y: 10, align: 'start' as const }, { x: 390, y: 10, align: 'start' as const }] };
        expect(placeSecondaryLabels([covered], [labelRect(30, 17, 60, 20, [0, 0])], 400, 300).get('c')).toBe(-1);
    });
    it('keeps every Behavior call name and switches all chips to the compact form when the full ones would overlap', () => {
        const full = { width: 116, height: 58 };
        // handle · loaddata.py in the hand-test window: neighbouring call boxes 60 px apart.
        expect(journeyChipForm([{ x: 500, y: 100 }, { x: 500, y: 160 }, { x: 500, y: 220 }], full)).toBe('compact');
        expect(journeyChipForm([{ x: 500, y: 100 }, { x: 500, y: 180 }, { x: 200, y: 140 }], full)).toBe('full');
    });
    it('leaves folder names where they are while the pointer moves over the chips (Hotspots, Overview)', () => {
        const chips: SceneChip[] = [{ id: 'create', x: 200, y: 100, width: 100, height: 24, measured: true }, { id: 'other', x: 600, y: 300, width: 100, height: 24, measured: true }];
        // The folder name's own corner lies just below the chip, its next corner further away.
        const folders: SecondaryLabel[] = [{ id: 'contrib', width: 60, height: 14, spots: [{ x: 160, y: 125, align: 'start' }, { x: 420, y: 200, align: 'start' }] }];
        const options = { cull: true, width: 800, height: 500, inset: [4, 3] as [number, number] };
        const resting = placeSceneLabels(chips, folders, options);
        expect(resting.folders.get('contrib')).toBe(0);
        // Hovering a chip makes it the priority chip, which is not a reason to move the names around it.
        const hovered = placeSceneLabels(chips, folders, { ...options, priorityId: 'create' });
        expect(hovered.folders).toEqual(resting.folders);
        // Nor to hide its neighbours: only a selected chip grows, a hovered one keeps its size (Hotspots: create hid label_for_field).
        const crowded: SceneChip[] = [...chips, { id: 'label_for_field', x: 230, y: 132, width: 110, height: 24, measured: true }];
        const calm = placeSceneLabels(crowded, [], options);
        expect(calm.visible.size).toBe(3);
        expect([...placeSceneLabels(crowded, [], { ...options, priorityId: 'create' }).visible].sort()).toEqual([...calm.visible].sort());
        // A selected chip opens its extra rows; the name gives way to it.
        expect(placeSceneLabels(chips, folders, { ...options, priorityId: 'create', selectedId: 'create' }).folders.get('contrib')).toBe(1);
    });
});
