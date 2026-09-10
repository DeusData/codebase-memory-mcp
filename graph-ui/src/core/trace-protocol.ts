/*
 * Herkunft: portiert am 2026-08-28 aus CodeAtlasIDE,
 * /Users/bernhard/Desktop/CodeAtlasIDE/theia-extensions/codeatlas-core/src/common/trace-protocol.ts
 * Gleicher Urheber (Bernhard Jackiewicz). Semantisch unveraendert uebernommen
 * wurden genau die zwei Funktionen, die `twin-view-model.ts` braucht:
 * `runtimeCitationsFor` und der Namensvergleich `sameSymbol`, den sie benutzt.
 *
 * Weggelassen: der ganze Rest der Datei (Ereignis-Schema, Speicherpfad,
 * Import-Bericht, die Bug-Pfad-DTOs, die Deckel). Der Grund ist nicht
 * Bequemlichkeit: dieses Projekt hat keinen Trace-Speicher und importiert
 * nichts, also gaebe es zu jenen Typen hier nichts, was sie beschreiben. Was
 * bleiben MUSS, ist die Zuordnung von Beobachtung zu Aufruf, weil der Twin sie
 * beim Zitieren einer Schritt-Zeile aufruft: liesse man sie weg und gaebe
 * stattdessen eine leere Liste zurueck, wuerde eine IR mit `runtime`-Fakt hier
 * still weniger Belege zeigen als im Referenzprojekt, und genau das ist die Art
 * Abweichung, die niemand mehr findet.
 *
 * Aenderungen gegenueber dem Original: keine, ausser den Importpfaden auf die
 * ebenfalls portierte semantic-ir.ts im selben Verzeichnis.
 */

import type { Evidence, SemanticIR } from './semantic-ir';

/**
 * The runtime citations that back one row of a call fact.
 *
 * One function, imported by both sides, because the backend's evidence lookup
 * and the twin's have to agree about which arrow a citation belongs to. They
 * already duplicate the fact-path grammar; duplicating the matching rule as well
 * would let the popover and the panel disagree about the same call.
 *
 * The match is by name: a stored event carries whatever the recorder wrote, and
 * a recorder that emits qualified names and one that emits bare names describe
 * the same call. So a bare `createUser` matches a qualified
 * `src.services.userService.createUser` and the other way round, and nothing
 * else does: a name that merely contains another is not a match, because
 * `createUserGroup` is a different function.
 */
export function runtimeCitationsFor(ir: SemanticIR, factPath: string): Evidence[] {
    const match = /^(steps|calls)\[(\d+)\]$/.exec(factPath);
    const runtime = ir.runtime;
    if (!match || runtime === undefined) {
        return [];
    }
    const calls = match[1] === 'steps' ? ir.steps.value : ir.calls.value;
    const call = calls[Number(match[2])];
    if (call === undefined) {
        return [];
    }
    // Only trust the row index when the two arrays line up, exactly as the
    // evidence lookups do: a mislabelled citation is worse than no citation.
    if (runtime.value.length !== runtime.evidence.length) {
        return [];
    }
    const out: Evidence[] = [];
    runtime.value.forEach((observed, index) => {
        if (sameSymbol(observed.targetName, call.targetName)
            || sameSymbol(observed.targetQualifiedName, call.targetQualifiedName)
            || sameSymbol(observed.targetQualifiedName, call.targetName)
            || sameSymbol(observed.targetName, call.targetQualifiedName)) {
            out.push(runtime.evidence[index]);
        }
    });
    return out;
}

/**
 * Whether two written names denote the same symbol.
 *
 * Equal, or one is the last dotted segment of the other. Nothing looser: a
 * substring rule would make `validateUser` match `validateUserInput`, and an
 * arrow attributed to the wrong function is the failure this whole feature
 * exists to avoid.
 */
export function sameSymbol(left: string | undefined, right: string | undefined): boolean {
    if (left === undefined || right === undefined || left.length === 0 || right.length === 0) {
        return false;
    }
    return left === right || left.endsWith(`.${right}`) || right.endsWith(`.${left}`);
}
