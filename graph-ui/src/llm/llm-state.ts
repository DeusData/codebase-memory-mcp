/**
 * Die eine Stelle, an der Policy und Praeferenz aufeinandertreffen.
 *
 * Eigene Datei und nicht ein Anhaengsel von policy.ts, weil hier zwei Quellen
 * zusammenkommen, die sonst nichts voneinander wissen: die committete Datei des
 * Zielprojekts und der Speicher dieses Browsers. Sie an genau einem Ort zu
 * verrechnen ist der Grund, warum die Vorrangregel pruefbar ist, ohne dass ein
 * Test eine Oberflaeche bauen muss.
 *
 * Die Regel in drei Zeilen:
 *
 * 1. Sperrt die Policy, ist das LLM aus und der Schalter wirkungslos.
 * 2. Sonst entscheidet die Praeferenz dieses Browsers.
 * 3. Solange die Policy noch keine Antwort gegeben hat, ist das LLM aus.
 *
 * Regel 3 ist die unauffaellige und die wichtigste. Ein Opt-out, das waehrend
 * des Nachschlagens schon "an" ist, hat in genau diesem Moment eine Anfrage
 * geschickt, die niemand erlaubt hat. Der Preis ist eine kurze Spanne, in der
 * das Panel "off" sagt, obwohl der Schalter an ist; das ist die Wahrheit, denn
 * gefragt wird in dieser Spanne nichts.
 */

import { blocksLlm } from './policy';
import type { PolicyVerdict } from './policy';

/**
 * Was aus Policy und Praeferenz folgt.
 *
 * `on` ist keine Lage des Sidecars, sondern die Erlaubnis, ihn zu fragen. Was
 * dabei herauskommt, sagt erst die Probe.
 */
export type LlmMode = 'off' | 'on' | 'disabled-by-policy';

/** Policy schlaegt Praeferenz, und eine unbeantwortete Policy schlaegt beide. */
export function resolveLlmState(
    verdict: PolicyVerdict | undefined,
    preferenceOn: boolean,
): LlmMode {
    if (verdict === undefined) {
        return 'off';
    }
    if (blocksLlm(verdict)) {
        return 'disabled-by-policy';
    }
    return preferenceOn ? 'on' : 'off';
}
