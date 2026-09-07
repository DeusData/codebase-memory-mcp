/* Tiny shared fetch used by why.ts (kept separate so the pure helpers in
 * why.ts stay importable in tests without a fetch mock). */
export async function fetchJsonFrom<T>(url: string): Promise<T> {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
  return res.json() as Promise<T>;
}
