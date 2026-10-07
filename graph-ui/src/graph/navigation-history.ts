/**
 * Ein begrenzter Verlauf mit Zurueck und Vor, wie beim Blaettern (K2).
 *
 * Rein und ohne React, damit Galaxy (K2) und Architecture (K27) dasselbe
 * Modell benutzen und nicht zwei. Das Konzept steht in
 * docs/development/pr-2068-galaxy-history.md; hier stehen nur die Regeln, die
 * der Code einhaelt:
 *
 *  - Hoechstens `limit` Eintraege (Vorgabe 25). Wer darueber hinaus schiebt,
 *    verliert den aeltesten, nicht den neuesten.
 *  - Ein Eintrag, der dem aktuellen gleicht (gleicher Schluessel), wird nicht
 *    noch einmal abgelegt. Derselbe Ort spaeter ist ein neuer Schritt.
 *  - Eine neue Navigation nach Zurueck verwirft den Vorwaertszweig.
 *  - Daneben eine kurze Liste der zuletzt besuchten Orte (`recentKey`), neueste
 *    zuerst und ohne Doppelte. Sie haengt nicht am Zeiger: Zurueck kuerzt sie
 *    nicht, ein Besuch zieht seinen Ort nach vorn.
 *
 * Gespeichert werden nur die Eintraege, die der Aufrufer baut. Was sie
 * bedeuten (welche Knoten, welche Kamera), weiss dieses Modul nicht, und es
 * waechst darum auch nicht mit dem, was ein Bild an Daten traegt.
 */

export interface NavigationHistory<T> {
    readonly entries: readonly T[];
    /** Der Eintrag, auf dem der Leser steht. -1 heisst: noch keiner. */
    readonly index: number;
    /** Die zuletzt besuchten Orte, neueste zuerst. */
    readonly recent: readonly T[];
}

export interface NavigationHistoryOptions<T> {
    /** Gleicher Schluessel heisst gleicher Eintrag. */
    key: (entry: T) => string;
    /** Der Ort eines Eintrags fuer die Liste der letzten Orte. Ohne Ort steht er nicht darin. */
    recentKey?: (entry: T) => string | undefined;
    limit?: number;
    recentLimit?: number;
}

export const NAVIGATION_HISTORY_LIMIT = 25;
export const NAVIGATION_RECENT_LIMIT = 8;

export function emptyNavigationHistory<T>(): NavigationHistory<T> {
    return { entries: [], index: -1, recent: [] };
}

function visit<T>(recent: readonly T[], entry: T, options: NavigationHistoryOptions<T>): readonly T[] {
    const place = options.recentKey?.(entry);
    if (place === undefined) return recent;
    const rest = recent.filter((candidate) => options.recentKey?.(candidate) !== place);
    return [entry, ...rest].slice(0, Math.max(1, options.recentLimit ?? NAVIGATION_RECENT_LIMIT));
}

/** Ein neuer Schritt. Gleicht er dem aktuellen, bleibt alles, wie es ist. */
export function pushNavigation<T>(history: NavigationHistory<T>, entry: T, options: NavigationHistoryOptions<T>): NavigationHistory<T> {
    const current = history.entries[history.index];
    if (current !== undefined && options.key(current) === options.key(entry)) return history;
    const limit = Math.max(1, options.limit ?? NAVIGATION_HISTORY_LIMIT);
    const kept = [...history.entries.slice(0, history.index + 1), entry];
    const entries = kept.slice(Math.max(0, kept.length - limit));
    return { entries, index: entries.length - 1, recent: visit(history.recent, entry, options) };
}

/**
 * Den aktuellen Eintrag ersetzen statt einen Schritt abzulegen (K27): fuer
 * das, was die Seite von selbst einstellt, etwa den vorgeschlagenen
 * Behavior-Start oder ein Zuruecksetzen nach dem Neuindizieren. Waere das ein
 * eigener Schritt, fuehrte Zurueck auf den leeren Zustand, die Seite stellte
 * ihn sofort wieder ein, und Zurueck kaeme nie darueber hinaus.
 *
 * Der Vorwaertszweig bleibt. Der ersetzte Ort verlaesst die Liste der letzten
 * Orte, wenn er dort vorn steht. Gleicht der neue Eintrag dem davor oder dem
 * danach, fallen sie zusammen, wie bei `pushNavigation`: sonst stuenden zwei
 * gleiche Schritte nebeneinander, und Vor oder Zurueck fuehrte an denselben
 * Ort. Ohne aktuellen Eintrag ist es ein gewoehnlicher erster Schritt.
 */
export function replaceNavigation<T>(history: NavigationHistory<T>, entry: T, options: NavigationHistoryOptions<T>): NavigationHistory<T> {
    const current = history.entries[history.index];
    if (current === undefined) return pushNavigation(history, entry, options);
    const key = options.key(entry);
    if (options.key(current) === key) return history;
    const replaced = options.recentKey?.(current);
    const front = history.recent[0];
    const recent = replaced !== undefined && front !== undefined && options.recentKey?.(front) === replaced ? history.recent.slice(1) : history.recent;
    const same = (other: T | undefined): other is T => other !== undefined && options.key(other) === key;
    const before = history.entries.slice(0, history.index);
    const after = history.entries.slice(history.index + (same(history.entries[history.index + 1]) ? 2 : 1));
    const previous = history.entries[history.index - 1];
    if (same(previous)) return { entries: [...before, ...after], index: history.index - 1, recent: visit(recent, previous, options) };
    return { entries: [...before, entry, ...after], index: history.index, recent: visit(recent, entry, options) };
}

/**
 * Den aktuellen Eintrag durch einen gleichen (gleicher Schluessel) mit neueren
 * Angaben ersetzen (K27), etwa einem Namen, der erst nach dem Schritt bekannt
 * wird. Kein neuer Schritt und kein Umsortieren der letzten Orte; dort steht
 * der Ort danach mit seinem neuesten Stand. Ein anderer Schluessel ist ein
 * Schritt und kein Auffrischen: dann bleibt alles, wie es ist.
 */
export function refreshNavigation<T>(history: NavigationHistory<T>, entry: T, options: NavigationHistoryOptions<T>): NavigationHistory<T> {
    const current = history.entries[history.index];
    if (current === undefined || current === entry || options.key(current) !== options.key(entry)) return history;
    const entries = history.entries.map((item, at) => (at === history.index ? entry : item));
    const place = options.recentKey?.(entry);
    const recent = place === undefined ? history.recent : history.recent.map((item) => (options.recentKey?.(item) === place ? entry : item));
    return { entries, index: history.index, recent };
}

/** Der Eintrag, zu dem Zurueck (-1) oder Vor (+1) fuehren wuerde. */
export function peekNavigation<T>(history: NavigationHistory<T>, step: -1 | 1): T | undefined {
    return history.index < 0 ? undefined : history.entries[history.index + step];
}

/** Einen Schritt zurueck oder vor. Ohne Ziel bleibt der Verlauf derselbe. */
export function moveNavigation<T>(history: NavigationHistory<T>, step: -1 | 1, options: NavigationHistoryOptions<T>): NavigationHistory<T> {
    const entry = peekNavigation(history, step);
    if (entry === undefined) return history;
    return { entries: history.entries, index: history.index + step, recent: visit(history.recent, entry, options) };
}
