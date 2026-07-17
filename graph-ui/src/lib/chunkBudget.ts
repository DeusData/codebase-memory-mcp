export const MAX_JS_CHUNK_BYTES = 750_000;

export interface ChunkSize {
  fileName: string;
  bytes: number;
}

export function chunkBudgetViolations(
  chunks: ChunkSize[],
  budgetBytes = MAX_JS_CHUNK_BYTES,
): ChunkSize[] {
  return chunks.filter((chunk) => chunk.bytes > budgetBytes);
}

export function assertChunkBudget(
  chunks: ChunkSize[],
  budgetBytes = MAX_JS_CHUNK_BYTES,
): void {
  const violations = chunkBudgetViolations(chunks, budgetBytes);
  if (violations.length === 0) return;
  const details = violations
    .map((chunk) => `${chunk.fileName} (${chunk.bytes} bytes)`)
    .join(", ");
  throw new Error(`JavaScript chunk budget exceeded (${budgetBytes} bytes): ${details}`);
}
