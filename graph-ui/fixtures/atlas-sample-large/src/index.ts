// Einstiegspunkt des Mess-Korpus. Er tut absichtlich fast nichts: gemessen
// wird der Reader an big.ts, nicht dieses Projekt.

import { runAll, step001 } from './big';
import { normalize } from './support';

/** Faehrt den generierten Korpus einmal durch und gibt das Ergebnis zurueck. */
export function main(seed = 1): number {
    const first = step001(seed);
    return normalize(runAll(first));
}
