/* Node ≥22 ships an experimental `localStorage` global that reads as
 * undefined unless --localstorage-file is passed. Vitest's jsdom environment
 * only overrides globals on its known-keys list, so Node's dead global
 * shadows jsdom's working Storage — and because the app treats storage as
 * best-effort (try/catch everywhere), every persistence path silently
 * no-ops in tests. Restore jsdom's real Storage objects. */
const g = globalThis as typeof globalThis & { jsdom?: { window: Window } };
if (g.jsdom && typeof g.localStorage === "undefined") {
  for (const key of ["localStorage", "sessionStorage"] as const) {
    Object.defineProperty(globalThis, key, {
      value: g.jsdom.window[key],
      configurable: true,
      writable: true,
    });
  }
}
