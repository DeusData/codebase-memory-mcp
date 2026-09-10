/**
 * Was der Build ueber sich selbst weiss.
 *
 * Die Version wird zur Buildzeit hineingeschrieben (vite.config.ts, `define`),
 * nicht zur Laufzeit erfragt: eine Oberflaeche, die im Browser nach ihrer
 * eigenen Version fragt, kann nur eine Antwort bekommen, die jemand anders
 * gebaut hat. Der Zusatz `-dirty` steht dran, wenn der Arbeitsbaum beim Bauen
 * nicht sauber war, und er ist genau dann wichtig, wenn ein Screenshot zu einer
 * Fassung gehoeren soll, die es als Commit nicht gibt.
 */

declare const __ATLAS_VERSION__: string;

/**
 * Was der Build zur Bauzeit ueber sich geschrieben hat: `v0.0.1`, `v0.0.1-dirty`
 * oder, ohne Vite, der Rueckfall. Er sagt `dev` statt eine Zahl zu erfinden.
 */
const stamped: string =
    typeof __ATLAS_VERSION__ === 'string' && __ATLAS_VERSION__.length > 0
        ? __ATLAS_VERSION__
        : 'v0.0.0-dev';

const dash = stamped.indexOf('-');

/**
 * Die Version allein, in der Form `v0.0.1`.
 *
 * Getrennt vom Zusatz, und das ist eine Entscheidung mit einem Grund: der Chip
 * beantwortet die Frage "welche Fassung ist das", und darauf ist `v0.0.1` die
 * Antwort. `v0.0.1-dirty` beantwortet zwei Fragen in einem Wort, und die zweite
 * ("stand beim Bauen etwas uncommitted") ist eine Aussage ueber den
 * Arbeitsbaum, nicht ueber die Fassung. Die Aussage verschwindet nicht, sie
 * bekommt ihr eigenes Element daneben (`ATLAS_BUILD_SUFFIX`).
 */
export const ATLAS_VERSION: string = dash === -1 ? stamped : stamped.slice(0, dash);

/**
 * Der Zusatz hinter der Version: `dirty`, `dev`, oder leer.
 *
 * Leer heisst "aus einem sauberen Baum gebaut" und nicht "keine Aussage": ohne
 * git faellt der Zusatz beim Bauen schon weg (vite.config.ts), und dieses Modul
 * erfindet nichts dazu.
 */
export const ATLAS_BUILD_SUFFIX: string = dash === -1 ? '' : stamped.slice(dash + 1);
