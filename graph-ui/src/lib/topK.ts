/** Select the top-`k` items by `size` (descending) without sorting the whole
 * input. NodeLabels previously did `[...nodes].sort(...).slice(0, k)` every
 * render, which clones and fully sorts the entire node array (up to
 * hundreds of thousands of nodes) just to keep the top 80. This keeps a
 * bounded min-heap of size k instead — O(n log k) instead of O(n log n),
 * and no full-array clone. */
export function topKBySize<T extends { size: number }>(
  items: T[],
  k: number,
): T[] {
  if (k <= 0) return [];
  if (items.length <= k) {
    return [...items].sort((a, b) => b.size - a.size);
  }

  /* Min-heap of size k, keyed by `size` — keeps the k largest items seen so
   * far; anything smaller than the current minimum is discarded in O(log k). */
  const heap: T[] = [];

  const siftUp = (start: number) => {
    let i = start;
    while (i > 0) {
      const parent = (i - 1) >> 1;
      if (heap[i].size < heap[parent].size) {
        [heap[i], heap[parent]] = [heap[parent], heap[i]];
        i = parent;
      } else {
        break;
      }
    }
  };

  const siftDown = (start: number) => {
    let i = start;
    for (;;) {
      const l = i * 2 + 1;
      const r = i * 2 + 2;
      let smallest = i;
      if (l < heap.length && heap[l].size < heap[smallest].size) smallest = l;
      if (r < heap.length && heap[r].size < heap[smallest].size) smallest = r;
      if (smallest === i) break;
      [heap[i], heap[smallest]] = [heap[smallest], heap[i]];
      i = smallest;
    }
  };

  for (const item of items) {
    if (heap.length < k) {
      heap.push(item);
      siftUp(heap.length - 1);
    } else if (item.size > heap[0].size) {
      heap[0] = item;
      siftDown(0);
    }
  }

  return heap.sort((a, b) => b.size - a.size);
}
