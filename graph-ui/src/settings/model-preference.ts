/**
 * Welches Modell dieser Browser fuer dieses Projekt gewaehlt hat.
 *
 * Derselbe Schluesselschnitt wie `atlas-llm:<projekt>` (src/llm/preference.ts)
 * und aus demselben Grund: dieses Frontend hat keinen Ort, an dem es fuer einen
 * Menschen etwas merken koennte, ausser dem Speicher dieses Browsers. Was das
 * schwaecher macht, steht in src/checklist/understanding-store.ts und gilt hier
 * unveraendert.
 *
 * ## Die eine Regel: fehlt der Eintrag, waehlt niemand
 *
 * Ein leerer Wert heisst nicht "nimm das erste Modell", sondern "der Leser hat
 * nichts gewaehlt". Der Unterschied ist die ganze Datei: ohne Wahl traegt eine
 * Chat-Anfrage KEIN Feld `model` und der Sidecar antwortet mit dem, was er
 * ohnehin geladen hat, genau wie vor W10. Eine Vorgabe hier waere eine stille
 * Entscheidung darueber, welches Modell laeuft, und genau die soll dieses
 * Produkt seit W10 nicht mehr treffen (docs/adr/0001, Nachtrag W10).
 *
 * ## Warum die Wahl nicht geprueft wird, wenn sie gelesen wird
 *
 * Sie ist eine id, kein Pfad, und ob es sie noch gibt, weiss nur der laufende
 * Prozess. Was zu tun ist, wenn die Wahl auf ein verschwundenes Modell zeigt,
 * entscheidet darum `activeModelOf` in src/llm/sidecar.ts, dort, wo die Liste
 * des Prozesses danebenliegt. Hier zu raten hiesse, den Speicher gegen eine
 * Liste zu pruefen, die diese Datei nicht hat.
 */

import type { KeyValueStore } from '../checklist/understanding-store';

/** Praefix des Schluessels, unter dem die Wahl eines Projekts liegt. */
export const MODEL_KEY_PREFIX = 'atlas-model:';

/** Der Schluessel, unter dem die Wahl eines Projekts liegt. */
export function modelKey(project: string): string {
    return `${MODEL_KEY_PREFIX}${project}`;
}

/** Was gespeichert wird. Ein Feld, weil es genau eine Entscheidung ist. */
export interface ModelPreference {
    /** Die id, die eine Anfrage mitschickt. Leer heisst: keine Wahl. */
    id: string;
}

const NONE: ModelPreference = { id: '' };

/**
 * Die Wahl dieses Browsers fuer dieses Projekt.
 *
 * Jeder Zweifel endet bei "keine Wahl": kein Eintrag, unlesbares JSON, ein
 * Speicher, der die Lesung verweigert, ein Wert, der keine Zeichenkette ist.
 */
export function readModelPreference(store: KeyValueStore, project: string): ModelPreference {
    if (project.length === 0) {
        return NONE;
    }
    let raw: string | null;
    try {
        raw = store.getItem(modelKey(project));
    } catch {
        return NONE;
    }
    if (raw === null) {
        return NONE;
    }
    try {
        const parsed = JSON.parse(raw) as Record<string, unknown>;
        const id = parsed['id'];
        return { id: typeof id === 'string' ? id : '' };
    } catch {
        return NONE;
    }
}

/** Die Wahl setzen. Liefert, was jetzt gilt. Eine leere id loescht die Wahl. */
export function recordModelPreference(
    store: KeyValueStore,
    project: string,
    id: string,
): ModelPreference {
    const preference: ModelPreference = { id };
    if (project.length === 0) {
        return preference;
    }
    try {
        store.setItem(modelKey(project), JSON.stringify(preference));
    } catch {
        // Ein verweigerter Speicher kostet den Leser das erneute Waehlen beim
        // naechsten Laden. Dasselbe Abwaegen wie bei der LLM-Praeferenz.
    }
    return preference;
}
