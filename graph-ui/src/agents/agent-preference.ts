/**
 * Was der Leser am Instrument eingestellt hat, und was davon den Reload
 * ueberlebt.
 *
 * Die Groesse ueberlebt ihn, weil der Contract es verlangt und weil es die
 * richtige Regel ist: wer das Instrument eingeklappt hat, hat entschieden, wie
 * viel Platz der Graph bekommen soll, und eine Entscheidung, die man bei jedem
 * Laden neu treffen muss, ist keine Einstellung, sondern eine Geste. Dasselbe
 * gilt fuer den Umschalter you/agent/both und fuer die drei Schalter des
 * laufenden Blicks.
 *
 * Der Live-Modus selbst steht NICHT hier. Er ist die Frage, ob eine Verbindung
 * aufgemacht wird, und die Vorgabe darauf ist "nein". Eine gespeicherte
 * Einschaltung waere eine Seite, die nach dem naechsten Laden von selbst zu
 * reden anfaengt; die Regel dieses Produkts ist, dass ein Leser das jedes Mal
 * selbst entscheidet, genau wie beim lokalen Modell.
 */

import type { ActorFilter, HudSize } from './agent-view';
import { TRAIL_WINDOWS } from './agent-view';

/** Praefix des Schluessels, unter dem die Wahl eines Projekts liegt. */
export const AGENTS_KEY_PREFIX = 'atlas-agents:';

/** Der Schluessel dieses Projekts. */
export function agentsKey(project: string): string {
    return `${AGENTS_KEY_PREFIX}${project}`;
}

/**
 * Was gespeichert wird.
 *
 * `fullscreen` hiess bis W11b `cinema`. Umbenannt auf ausdruecklichen
 * Nutzerwunsch vom 2026-08-30 ("bin jetzt im cinema mode, sollte fullscreen
 * heissen"), und zwar ueberall: am Schalter, in der Beschriftung, in der
 * Kommandozeile und an der Naht. Ein gespeicherter Eintrag von vorher traegt
 * den alten Schluessel; er faellt damit auf die Vorgabe zurueck, und das ist
 * hier richtig, weil das Vollbild ohnehin bei jedem Laden eine Entscheidung
 * sein soll.
 */
export interface AgentsPreference {
    size: HudSize;
    filter: ActorFilter;
    follow: boolean;
    trails: boolean;
    fullscreen: boolean;
    /**
     * Das Fenster, ueber das die Wegzeile, die Spur und der Zeitstrahl gehen.
     *
     * EINE Zahl fuer alle drei, und nicht drei nebeneinander: sie beantworten
     * dieselbe Frage ("wie weit zurueck?"), und der Umschalter steht im
     * Designbild genau einmal, am Zeitstrahl. Drei Fenster, die man einzeln
     * stellen kann, waeren drei Antworten auf eine Frage.
     */
    trailWindowMs: number;
}

/**
 * Die Vorgabe.
 *
 * Kompakt, beide Akteursarten, und die drei Schalter aus. `follow` aus, weil
 * eine Kamera, die von selbst losfaehrt, dem Leser die Ansicht nimmt, die er
 * gerade eingestellt hat; `fullscreen` aus, weil das Vollbild eine Entscheidung
 * ist; `trails` aus, weil die Wegzeile die Zeilen hoeher macht und das
 * Instrument im Normalzustand kompakt sein soll.
 */
export const DEFAULT_AGENTS_PREFERENCE: AgentsPreference = {
    size: 'compact',
    filter: 'both',
    follow: false,
    trails: false,
    fullscreen: false,
    trailWindowMs: TRAIL_WINDOWS[0] as number,
};

/** Nur das, was dieses Modul von einem Speicher braucht. */
export interface PreferenceStore {
    getItem(key: string): string | null;
    setItem(key: string, value: string): void;
}

function asOneOf<T>(value: unknown, allowed: readonly T[], fallback: T): T {
    return allowed.includes(value as T) ? (value as T) : fallback;
}

function asBoolean(value: unknown, fallback: boolean): boolean {
    return typeof value === 'boolean' ? value : fallback;
}

/**
 * Was aus dem Speicher kam, auf zulaessige Werte zurechtstutzen.
 *
 * Jeder unbekannte Wert faellt auf die Vorgabe zurueck, wie in
 * src/galaxy/density.ts und aus demselben Grund: die Vorgabe ist die einzige
 * Zahl, ueber die dieses Modul etwas weiss.
 */
export function clampAgentsPreference(raw: unknown): AgentsPreference {
    const record = typeof raw === 'object' && raw !== null ? (raw as Record<string, unknown>) : {};
    return {
        size: asOneOf<HudSize>(
            record['size'], ['collapsed', 'compact', 'expanded'], DEFAULT_AGENTS_PREFERENCE.size,
        ),
        filter: asOneOf<ActorFilter>(
            record['filter'], ['you', 'agent', 'both'], DEFAULT_AGENTS_PREFERENCE.filter,
        ),
        follow: asBoolean(record['follow'], DEFAULT_AGENTS_PREFERENCE.follow),
        trails: asBoolean(record['trails'], DEFAULT_AGENTS_PREFERENCE.trails),
        fullscreen: asBoolean(record['fullscreen'], DEFAULT_AGENTS_PREFERENCE.fullscreen),
        trailWindowMs: asOneOf(
            record['trailWindowMs'], TRAIL_WINDOWS, DEFAULT_AGENTS_PREFERENCE.trailWindowMs,
        ),
    };
}

/** Die gespeicherte Wahl dieses Browsers fuer dieses Projekt. */
export function loadAgentsPreference(
    store: PreferenceStore | undefined,
    project: string,
): AgentsPreference {
    if (store === undefined || project.length === 0) {
        return { ...DEFAULT_AGENTS_PREFERENCE };
    }
    let raw: string | null;
    try {
        raw = store.getItem(agentsKey(project));
    } catch {
        return { ...DEFAULT_AGENTS_PREFERENCE };
    }
    if (raw === null) {
        return { ...DEFAULT_AGENTS_PREFERENCE };
    }
    try {
        return clampAgentsPreference(JSON.parse(raw));
    } catch {
        return { ...DEFAULT_AGENTS_PREFERENCE };
    }
}

/** Die Wahl speichern. Liefert, was jetzt gilt. */
export function saveAgentsPreference(
    store: PreferenceStore | undefined,
    project: string,
    preference: AgentsPreference,
): AgentsPreference {
    if (store === undefined || project.length === 0) {
        return preference;
    }
    try {
        store.setItem(agentsKey(project), JSON.stringify(preference));
    } catch {
        // Ein verweigerter Speicher kostet den Leser das erneute Einstellen beim
        // naechsten Laden. Dasselbe Abwaegen wie bei den Darstellungswerten.
    }
    return preference;
}
