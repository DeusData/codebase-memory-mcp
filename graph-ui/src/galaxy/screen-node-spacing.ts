export interface ScreenNode {
    id: number;
    x: number;
    y: number;
    radius: number;
}

type Disk = { x: number; y: number; radius: number };
type Level = { width: number; columns: Map<number, Map<number, Disk[]>> };
const CLEARANCE = .001;
const SEARCH_RAYS = 4;
const PUSHES_PER_RAY = 24;
const GOLDEN_ANGLE = Math.PI * (3 - Math.sqrt(5));

function directionSeed(id: number): number {
    let seed = Math.imul(id ^ (id >>> 16), 0x45d9f3b);
    seed = Math.imul(seed ^ (seed >>> 16), 0x45d9f3b);
    return ((seed ^ (seed >>> 16)) >>> 0) / 0x100000000 * Math.PI * 2;
}

/** Screen-space presentation only. Pinned disks (a scope root) never move and
 * are placed first; then the largest disks, preserving their preferred
 * positions when possible. Existing graph IDs and metadata are retained, and
 * the result remains in the original input order.
 *
 * Each radius scale has its own spatial hash. Earlier disks are at least as
 * large as the candidate, so only nine cells per occupied scale can overlap;
 * pinned disks are hashed at the largest scale for the same guarantee.
 * Local search is bounded; an outside-bounds fallback guarantees clearance
 * without dropping nodes or squeezing them into a fixed viewport. */
export function separateScreenNodes<T extends ScreenNode>(nodes: readonly T[], gap = 4, pinned: ReadonlySet<number> = new Set()): T[] {
    if (!Number.isFinite(gap) || gap < 0 || nodes.some(node => ![node.x, node.y, node.radius].every(Number.isFinite) || node.radius < 0)) {
        throw new RangeError('Screen node positions, radii and gap must be finite; radii and gap must be nonnegative.');
    }
    const order = nodes.map((node, index) => ({ node, index, pinned: pinned.has(node.id) }))
        .sort((a, b) => Number(b.pinned) - Number(a.pinned) || b.node.radius - a.node.radius || a.node.id - b.node.id || a.index - b.index);
    const largest = nodes.reduce((max, node) => Math.max(max, node.radius + gap / 2), 0);
    const result = new Array<T>(nodes.length), levels = new Map<number, Level>();
    let left = Infinity, right = -Infinity, top = Infinity, bottom = -Infinity;

    const collisions = (x: number, y: number, radius: number): Disk[] => {
        const found: Disk[] = [];
        for (const level of levels.values()) {
            const cx = Math.floor(x / level.width), cy = Math.floor(y / level.width);
            for (let ix = cx - 1; ix <= cx + 1; ix++) {
                const column = level.columns.get(ix); if (!column) continue;
                for (let iy = cy - 1; iy <= cy + 1; iy++) {
                    const bucket = column.get(iy); if (!bucket) continue;
                    for (const other of bucket) {
                        const dx = x - other.x, dy = y - other.y, distance = radius + other.radius;
                        if (dx * dx + dy * dy < distance * distance) found.push(other);
                    }
                }
            }
        }
        return found;
    };

    const insert = (disk: Disk, scale = disk.radius) => {
        left = Math.min(left, disk.x - disk.radius); right = Math.max(right, disk.x + disk.radius);
        top = Math.min(top, disk.y - disk.radius); bottom = Math.max(bottom, disk.y + disk.radius);
        if (disk.radius === 0) return; // Zero-area disks cannot obstruct later zero-area disks.
        const width = 2 ** Math.ceil(Math.log2(scale * 2));
        let level = levels.get(width);
        if (!level) { level = { width, columns: new Map() }; levels.set(width, level); }
        const x = Math.floor(disk.x / width), y = Math.floor(disk.y / width);
        let column = level.columns.get(x); if (!column) { column = new Map(); level.columns.set(x, column); }
        const bucket = column.get(y) ?? []; bucket.push(disk); column.set(y, bucket);
    };

    for (const { node, index, pinned: fixed } of order) {
        const radius = node.radius + gap / 2;
        if (fixed) { result[index] = node; insert({ x: node.x, y: node.y, radius }, largest); continue; }
        const blocked = collisions(node.x, node.y, radius);
        let placed = { x: node.x, y: node.y };
        if (blocked.length) {
            let deepest = blocked[0]!, lowerBound = -Infinity;
            for (const other of blocked) {
                const required = radius + other.radius - Math.hypot(node.x - other.x, node.y - other.y);
                if (required > lowerBound) { deepest = other; lowerBound = required; }
            }
            const awayX = node.x - deepest.x, awayY = node.y - deepest.y;
            const angle = Math.hypot(awayX, awayY) > CLEARANCE ? Math.atan2(awayY, awayX) : directionSeed(node.id);
            let bestDistance = Infinity;
            for (let ray = 0; ray < SEARCH_RAYS; ray++) {
                const ux = Math.cos(angle + ray * GOLDEN_ANGLE), uy = Math.sin(angle + ray * GOLDEN_ANGLE);
                let distance = 0;
                for (let attempt = 0; attempt < PUSHES_PER_RAY; attempt++) {
                    const x = node.x + ux * distance, y = node.y + uy * distance;
                    const obstacles = attempt === 0 ? blocked : collisions(x, y, radius);
                    if (!obstacles.length) {
                        if (distance < bestDistance) { placed = { x, y }; bestDistance = distance; }
                        break;
                    }
                    // Jump beyond each blocking disk's exit along this ray;
                    // fixed pixel steps could stall inside a very large node.
                    let next = distance;
                    for (const other of obstacles) {
                        const dx = other.x - node.x, dy = other.y - node.y;
                        const along = dx * ux + dy * uy, perpendicular = dx * uy - dy * ux;
                        const sum = radius + other.radius;
                        next = Math.max(next, along + Math.sqrt(Math.max(0, sum * sum - perpendicular * perpendicular)) + CLEARANCE);
                    }
                    distance = next;
                    if (distance >= bestDistance) break;
                }
                if (bestDistance <= lowerBound + CLEARANCE * 2) break;
            }
            if (!Number.isFinite(bestDistance)) {
                const candidates = [
                    { x: left - radius - CLEARANCE, y: node.y }, { x: right + radius + CLEARANCE, y: node.y },
                    { x: node.x, y: top - radius - CLEARANCE }, { x: node.x, y: bottom + radius + CLEARANCE },
                ];
                placed = candidates.reduce((best, candidate) => Math.hypot(candidate.x - node.x, candidate.y - node.y)
                    < Math.hypot(best.x - node.x, best.y - node.y) ? candidate : best);
            }
        }
        result[index] = placed.x === node.x && placed.y === node.y ? node : { ...node, x: placed.x, y: placed.y };
        insert({ ...placed, radius });
    }
    return result;
}
