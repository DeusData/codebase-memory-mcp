/**
 * Die Praeferenz dieses Browsers zum lokalen Modell, je Projekt.
 *
 * Der Schluessel ist `atlas-llm:<projekt>`, dieselbe Form wie
 * `atlas-why:<projekt>` und `atlas-understanding:<projekt>`, und aus demselben
 * Grund: dieses Frontend hat keinen Ort, an dem es fuer einen Menschen etwas
 * merken koennte, ausser dem Speicher dieses Browsers. Was das schwaecher macht,
 * steht in src/checklist/understanding-store.ts und gilt hier unveraendert.
 *
 * Die eine Regel, die diese Datei traegt: **fehlt der Eintrag, ist das LLM aus.**
 * Nicht "unbekannt, also mal schauen" und nicht "an, bis jemand widerspricht".
 * Ein Erststart darf keinen Prozess suchen und keinen Port anfassen, und das
 * faengt bei der Frage an, was ein fehlender Wert bedeutet. Aus demselben Grund
 * zaehlt auch ein Wert, den diese Fassung nicht lesen kann, als aus: bei einem
 * Opt-out ist der Zweifel die Abwesenheit.
 */

import type { KeyValueStore } from '../checklist/understanding-store';

/** Praefix des Schluessels, unter dem die Praeferenz eines Projekts liegt. */
export const LLM_KEY_PREFIX = 'atlas-llm:';

/** Der Schluessel, unter dem die Praeferenz eines Projekts liegt. */
export function llmKey(project: string): string {
    return `${LLM_KEY_PREFIX}${project}`;
}

/** Was gespeichert wird. Ein Feld, weil es genau eine Entscheidung ist. */
export interface LlmPreference {
    on: boolean;
}

const OFF: LlmPreference = { on: false };

/**
 * Was dieser Browser fuer dieses Projekt eingestellt hat.
 *
 * Jeder Zweifel endet bei `off`: kein Eintrag, unlesbares JSON, ein Speicher,
 * der die Lesung verweigert, ein Wert, der kein Boolean ist. Der Preis ist ein
 * Leser, der seinen Schalter noch einmal umlegt; der Preis der Gegenrichtung
 * waere ein Prozess, den niemand angefordert hat.
 */
export function readLlmPreference(store: KeyValueStore, project: string): LlmPreference {
    if (project.length === 0) {
        return OFF;
    }
    let raw: string | null;
    try {
        raw = store.getItem(llmKey(project));
    } catch {
        return OFF;
    }
    if (raw === null) {
        return OFF;
    }
    try {
        const parsed = JSON.parse(raw) as Record<string, unknown>;
        return { on: parsed['on'] === true };
    } catch {
        return OFF;
    }
}

/** Die Praeferenz setzen. Liefert, was jetzt gilt. */
export function recordLlmPreference(
    store: KeyValueStore,
    project: string,
    on: boolean,
): LlmPreference {
    const preference: LlmPreference = { on };
    if (project.length === 0) {
        return preference;
    }
    try {
        store.setItem(llmKey(project), JSON.stringify(preference));
    } catch {
        // Ein verweigerter Speicher kostet den Leser das erneute Umlegen beim
        // naechsten Laden. Das ist ein besserer Ausgang als ein Schalter, der
        // sich nicht bewegt, weil das Merken nicht ging.
    }
    return preference;
}
