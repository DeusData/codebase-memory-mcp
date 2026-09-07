/* Squarified treemap layout (Bruls, Huizing & van Wijk 2000) — in-house so
 * the Modules screen needs no new dependency. Deterministic: input order is
 * preserved inside rows; items should arrive value-sorted descending. */

export interface TreemapItem {
  id: string;
  value: number;
}

export interface TreemapRect {
  id: string;
  x: number;
  y: number;
  w: number;
  h: number;
}

/* Worst aspect ratio a row would have if `row` (+lengths) shared `side`. */
function worst(row: number[], side: number, total: number): number {
  if (row.length === 0 || total <= 0) return Infinity;
  const sum = row.reduce((a, b) => a + b, 0);
  const max = Math.max(...row);
  const min = Math.min(...row);
  const s2 = sum * sum;
  const sideSq = side * side;
  return Math.max((sideSq * max) / s2, s2 / (sideSq * min));
}

export function squarify(
  items: TreemapItem[],
  x: number,
  y: number,
  w: number,
  h: number,
): TreemapRect[] {
  const positive = items.filter((item) => item.value > 0);
  const total = positive.reduce((a, item) => a + item.value, 0);
  if (positive.length === 0 || total <= 0 || w <= 0 || h <= 0) return [];
  /* Scale values to areas within the rect. */
  const scale = (w * h) / total;
  let queue = positive.map((item) => ({ id: item.id, area: item.value * scale }));
  const out: TreemapRect[] = [];
  let cx = x;
  let cy = y;
  let cw = w;
  let ch = h;

  while (queue.length > 0) {
    const side = Math.min(cw, ch);
    const row: typeof queue = [];
    let rowAreas: number[] = [];
    /* Grow the row while the worst aspect ratio improves. */
    while (queue.length > 0) {
      const candidate = [...rowAreas, queue[0].area];
      if (
        rowAreas.length === 0 ||
        worst(candidate, side, 1) <= worst(rowAreas, side, 1)
      ) {
        row.push(queue[0]);
        rowAreas = candidate;
        queue = queue.slice(1);
      } else {
        break;
      }
    }
    const rowArea = rowAreas.reduce((a, b) => a + b, 0);
    if (cw >= ch) {
      /* Vertical strip on the left. */
      const stripW = rowArea / ch;
      let oy = cy;
      for (const item of row) {
        const cellH = item.area / stripW;
        out.push({ id: item.id, x: cx, y: oy, w: stripW, h: cellH });
        oy += cellH;
      }
      cx += stripW;
      cw -= stripW;
    } else {
      /* Horizontal strip on top. */
      const stripH = rowArea / cw;
      let ox = cx;
      for (const item of row) {
        const cellW = item.area / stripH;
        out.push({ id: item.id, x: ox, y: cy, w: cellW, h: stripH });
        ox += cellW;
      }
      cy += stripH;
      ch -= stripH;
    }
  }
  return out;
}
