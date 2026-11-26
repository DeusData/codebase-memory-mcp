/** Keep small levels as columns; wrap large levels into compact ordered blocks. */
export function hierarchyBlockPositions<T>(
    levels: readonly { level: number; entries: readonly T[] }[],
    spacing: { levelGap: number; rowGap: number; nodeGap?: number; wrapAt?: number },
): { node: T; level: number; x: number; y: number }[] {
    const positions: { node: T; level: number; x: number; y: number }[] = [];
    const nodeGap = spacing.nodeGap ?? spacing.rowGap * 1.25;
    let previous: { level: number; right: number } | undefined;
    for (const { level, entries } of [...levels].sort((a, b) => a.level - b.level)) {
        if (!entries.length) continue;
        const columns = entries.length <= (spacing.wrapAt ?? 12) ? 1 : Math.ceil(Math.sqrt(entries.length));
        const rows = Math.ceil(entries.length / columns), width = (columns - 1) * nodeGap;
        const left = previous ? previous.right + (level - previous.level) * spacing.levelGap
            : level * spacing.levelGap - width / 2;
        previous = { level, right: left + width };
        entries.forEach((node, index) => positions.push({ node, level,
            x: left + index % columns * nodeGap,
            y: ((rows - 1) / 2 - Math.floor(index / columns)) * spacing.rowGap }));
    }
    return positions;
}
