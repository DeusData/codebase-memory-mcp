/** Screen space in canvas pixels. */
export interface LabelRect { left: number; right: number; top: number; bottom: number }
/** A label laid out in the DOM: its size, and the padding plus border between its edge and its text. */
export interface LabelBox { width: number; height: number; edgeX: number; edgeY: number }

/** The padding plus border of a laid-out label on its thinner side, per axis. */
export function labelEdges(style: Pick<CSSStyleDeclaration, 'getPropertyValue'>): [number, number] {
    const side = (name: string) => (parseFloat(style.getPropertyValue(`padding-${name}`)) || 0)
        + (parseFloat(style.getPropertyValue(`border-${name}-width`)) || 0);
    return [Math.min(side('left'), side('right')), Math.min(side('top'), side('bottom'))];
}

/**
 * How far a measured label may reach under a neighbour: half of the thinnest
 * padding plus border among the labels, so two neighbours together never
 * reach the text of either.
 */
export function labelInset(boxes: Iterable<LabelBox>): [number, number] {
    let x = Infinity, y = Infinity;
    for (const box of boxes) { x = Math.min(x, box.edgeX); y = Math.min(y, box.edgeY); }
    return [Number.isFinite(x) ? x / 2 : 0, Number.isFinite(y) ? y / 2 : 0];
}

/** The space a label centred at (x, y) claims; a negative inset keeps a margin around an estimate. */
export function labelRect(x: number, y: number, width: number, height: number, [insetX, insetY]: [number, number]): LabelRect {
    return { left: x - width / 2 + insetX, right: x + width / 2 - insetX, top: y - height / 2 + insetY, bottom: y + height / 2 - insetY };
}

export const labelsCollide = (a: LabelRect, b: LabelRect): boolean =>
    a.left < b.right && a.right > b.left && a.top < b.bottom && a.bottom > b.top;

/** Where a folder or lane name may sit: its top-left ('start') or top-right ('end') corner on the point. */
export interface LabelSpot { x: number; y: number; align: 'start' | 'end' }
export interface SecondaryLabel { id: string; width: number; height: number; spots: readonly LabelSpot[] }

export function spotRect(spot: LabelSpot, width: number, height: number): LabelRect {
    const left = spot.align === 'start' ? spot.x : spot.x - width;
    return { left, right: left + width, top: spot.y, bottom: spot.y + height };
}

/**
 * Folder and lane names come after the chips. Each takes the first of its
 * spots that stays inside the canvas and touches no label placed before it
 * (with a small margin), or stays hidden: a name under a chip reads as
 * "Outsi ◉ (root)", two names on one corner as "djdbmodels". Returns the
 * chosen spot per label, -1 for hidden.
 */
export function placeSecondaryLabels(labels: readonly SecondaryLabel[], occupied: readonly LabelRect[], width: number, height: number, margin = 3): Map<string, number> {
    const taken = [...occupied];
    const result = new Map<string, number>();
    for (const label of labels) {
        const chosen = label.spots.findIndex(spot => {
            const rect = spotRect(spot, label.width, label.height);
            const padded = { left: rect.left - margin, right: rect.right + margin, top: rect.top - margin, bottom: rect.bottom + margin };
            return rect.left >= 0 && rect.top >= 0 && rect.right <= width && rect.bottom <= height && !taken.some(other => labelsCollide(padded, other));
        });
        result.set(label.id, chosen);
        if (chosen >= 0) taken.push(spotRect(label.spots[chosen]!, label.width, label.height));
    }
    return result;
}

/** A map chip at its projected anchor; an unmeasured one carries an estimate and keeps a margin. */
export interface SceneChip { id: string; x: number; y: number; width: number; height: number; measured: boolean }
export interface SceneLabelOptions {
    /** The chip that claims space first and is never culled: the selected one, else the one under the pointer. */
    priorityId?: string;
    /** The selected chip, which shows its extra rows. */
    selectedId?: string;
    cull: boolean; width: number; height: number;
    /** labelInset of the measured chips. */
    inset: [number, number];
}

/**
 * Chips first, in the given order with the priority chip in front: with
 * `cull` a chip that leaves the canvas or meets one placed before it hides.
 * Only the selected chip reserves the 96 px its extra rows take; a hovered
 * chip goes first but keeps its size, so hovering a shown chip hides none of
 * its neighbours, and hovering a box reveals its hidden chip. Folder names
 * then take the free corners (placeSecondaryLabels), placed around the chips
 * as they lie without the pointer: hovering must not make the names beside a
 * chip jump to another corner and back.
 */
export function placeSceneLabels(chips: readonly SceneChip[], folders: readonly SecondaryLabel[], options: SceneLabelOptions): { visible: Set<string>; folders: Map<string, number> } {
    const place = (priorityId: string | undefined) => {
        const occupied: LabelRect[] = [], boxes: LabelRect[] = [], visible = new Set<string>();
        const ordered = [...chips].sort((a, b) => Number(b.id === priorityId) - Number(a.id === priorityId));
        for (const chip of ordered) {
            const height = chip.id === options.selectedId ? 96 : chip.height;
            const rect = labelRect(chip.x, chip.y, chip.width, height, chip.measured ? options.inset : [-5, 0]);
            if (options.cull && chip.id !== priorityId && (rect.right < 0 || rect.left > options.width || rect.bottom < 0 || rect.top > options.height
                || occupied.some(other => labelsCollide(rect, other)))) continue;
            // A folder name may not sit under any part of a chip: its whole box counts.
            visible.add(chip.id); occupied.push(rect); boxes.push(labelRect(chip.x, chip.y, chip.width, height, [0, 0]));
        }
        return { visible, boxes };
    };
    const chipsShown = place(options.priorityId);
    const resting = options.priorityId === options.selectedId ? chipsShown : place(options.selectedId);
    return { visible: chipsShown.visible, folders: placeSecondaryLabels(folders, resting.boxes, options.width, options.height) };
}

/**
 * Behavior call boxes never lose their name to a neighbour. When the full
 * chips (kind, name, file) would overlap at this zoom, every chip switches to
 * the compact form with the name alone; the tooltip keeps the rest.
 */
export function journeyChipForm(centres: readonly { x: number; y: number }[], full: { width: number; height: number }, gap = 4): 'full' | 'compact' {
    const rects = centres.map(({ x, y }) => labelRect(x, y, full.width + gap, full.height + gap, [0, 0]));
    return rects.some((rect, index) => rects.slice(index + 1).some(other => labelsCollide(rect, other))) ? 'compact' : 'full';
}
