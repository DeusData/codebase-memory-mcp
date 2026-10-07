/**
 * Der Galaxy-Eintrag fuer den Verlauf (K2): welche Frage der Ausschnitt gerade
 * beantwortet, ohne die Antwort selbst.
 *
 * Ein Eintrag haelt Identitaeten und Parameter (Wurzel, Tiefe, Richtung,
 * Kantenarten, offener Pfad, Ansicht) und nie geladene Knoten, Kanten oder eine
 * Kamera. Zurueck laedt den Ausschnitt neu; der kleine Scope-Cache und der
 * Nachbarschafts-Cache machen das in der Regel sofort. Das Modell dahinter ist
 * src/graph/navigation-history.ts, das Konzept steht in
 * docs/development/pr-2068-galaxy-history.md.
 */
import type { NavigationHistoryOptions } from '../graph/navigation-history';
import type { GraphScope, TraceDirection } from './graph-scope';
import { galaxyHistoryText as text } from './galaxy-strings';
import { scopeDisplayName } from './node-names';

export type ScopeTrail = { kind: 'path'; target: number; name: string } | { kind: 'calls' };

export interface GalaxyHistoryEntry {
    /** Fehlt sie, ist es der ganze Graph ("All graph"). */
    scope?: GraphScope;
    depth: number;
    direction: TraceDirection;
    /** Fehlt die Liste, sind alle Kantenarten verfolgt. */
    edgeTypes?: readonly string[];
    trail?: ScopeTrail;
    mode: 'galaxy' | 'hierarchy';
}

/** Wer die Wurzel ist, unabhaengig davon, ob sie per Klick oder per Suche kam. */
export function scopeIdentity(scope: GraphScope): string {
    if (scope.kind === 'node') return scope.qualifiedName ? `qn:${scope.qualifiedName}` : `node:${scope.id}`;
    if (scope.kind === 'symbol') return `qn:${scope.qualifiedName}`;
    return `${scope.kind}:${scope.path}`;
}

function trailKey(trail: ScopeTrail | undefined): string {
    return trail === undefined ? '' : trail.kind === 'calls' ? 'calls' : `path:${trail.target}`;
}

export const galaxyHistoryOptions: NavigationHistoryOptions<GalaxyHistoryEntry> = {
    key: (entry) => JSON.stringify([entry.scope ? scopeIdentity(entry.scope) : null, entry.scope ? entry.depth : 0,
        entry.scope ? entry.direction : 'both', entry.edgeTypes ? [...entry.edgeTypes].sort() : null, trailKey(entry.trail), entry.mode]),
    recentKey: (entry) => (entry.scope ? scopeIdentity(entry.scope) : undefined),
};

/** Wie ein Eintrag in einem Tooltip heisst: "n1 · 2 layers · path to n4". */
export function historyEntryLabel(entry: GalaxyHistoryEntry): string {
    return entry.scope ? `${scopeDisplayName(entry.scope)} · ${historyEntryDetail(entry)}` : text.allGraph;
}

/** Dasselbe ohne den Namen, fuer die Zeile unter dem Namen in der Liste der letzten Wurzeln. */
export function historyEntryDetail(entry: GalaxyHistoryEntry): string {
    if (!entry.scope) return text.allGraph;
    const parts = [text.layers(entry.depth)];
    if (entry.direction !== 'both') parts.push(text.direction[entry.direction]);
    if (entry.edgeTypes) parts.push(entry.edgeTypes.length ? entry.edgeTypes.join(', ') : text.noTypes);
    if (entry.mode === 'hierarchy') parts.push(text.hierarchy);
    if (entry.trail) parts.push(entry.trail.kind === 'calls' ? text.callOrder : text.pathTo(entry.trail.name));
    return parts.join(' · ');
}
