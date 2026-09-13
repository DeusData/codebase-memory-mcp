// Zwei Hilfsfunktionen, die big.ts aufruft. Sie sind hier, damit die grosse
// Datei nicht beziehungslos im Graphen haengt: der Indexer sieht Importe und
// Aufrufkanten und der Reader hat neben big.ts noch etwas Kleines zu zeigen.

/** Haelt einen Wert in einem festen Fenster, damit runAll nicht davonlaeuft. */
export function normalize(value: number): number {
    if (!Number.isFinite(value)) {
        return 0;
    }
    return ((value % 997) + 997) % 997;
}

/** Gewichtet einen Wert und normalisiert ihn danach. */
export function weigh(value: number): number {
    return normalize(Math.trunc(value / 3) + 11);
}
