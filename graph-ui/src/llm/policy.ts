/**
 * Die committete Projekt-Policy und ihr Vorrang vor jeder Nutzerpraeferenz.
 *
 * Das Referenzprojekt kennt eine `.codeatlas/policy.json` im Zielrepository, mit
 * der ein Team Faehigkeiten der IDE fuer dieses Repository abschaltet. Die
 * Mechanik gilt hier sinngemaess und mit einer scharfen Grenze: die Datei gehoert
 * dem INDEXIERTEN Projekt, nicht diesem Frontend. Sie wird also nicht vom
 * Dateisystem gelesen, sondern ueber denselben Weg, ueber den auch der Reader an
 * Quelltext kommt.
 *
 * ## Der Lese-Weg, gemessen und nicht vermutet
 *
 * `get_code_snippet` auf dem Modul-Knoten der Datei. Der Indexer legt fuer
 * `.codeatlas/policy.json` einen Modul-Knoten `<projekt>.codeatlas.policy` mit
 * Zeilenspanne an (gemessen am laufenden Server, verification/w5/sidecar.json,
 * Feld `policyReadPath`), und das Snippet liefert die ganze Datei. Genau das tut
 * `loadFileDocument` aus src/reader/file-source.ts, also wird es benutzt und
 * nicht nachgebaut: ein zweiter Weg zu einer Datei waere eine zweite Stelle, an
 * der eine Kappung gedeutet wird.
 *
 * `search_code` waere der andere gangbare Weg (er findet die Zeile `"llm":
 * "deny"` als Variablen-Knoten mit 60-Zeilen-Fenster). Er ist NICHT genommen
 * worden, weil er eine Zeile liefert statt einer Datei: eine Policy, von der man
 * nur die Trefferzeile kennt, laesst sich nicht als JSON pruefen, und ein
 * Regulaerausdruck auf `deny` wuerde auch in einem Kommentar anschlagen.
 *
 * ## Die Vorrangregel
 *
 * Die Policy schlaegt die Praeferenz, nie umgekehrt. Eine Praeferenz kann ein
 * `deny` nicht aufheben, und der Schalter bleibt sichtbar, wird aber
 * wirkungslos: ein verschwundener Schalter waere die Behauptung, es gaebe die
 * Einstellung nicht.
 *
 * Eine vorhandene, aber unlesbare Policy sperrt ebenfalls. Das ist die einzige
 * Stelle, an der diese Datei streng ist, und sie ist es mit Absicht: eine
 * Steuerungsdatei, die man nicht lesen kann, ist keine Erlaubnis. Der Satz auf
 * dem Bildschirm nennt dann die Datei und den Grund, damit die Sperre nicht
 * raetselhaft ist. Eine FEHLENDE Datei sperrt nicht: dort hat niemand etwas
 * gesagt, und dann gilt die Praeferenz.
 */

import { FileNotReadableError, loadFileDocument } from '../reader/file-source';
import type { RpcIntelligenceClient } from '../provider/rpc-client';

/** Wo die Policy im Zielprojekt liegt. Derselbe Pfad wie im Referenzprojekt. */
export const POLICY_PATH = '.codeatlas/policy.json';

/** Das Werkzeug, ueber das die Datei geholt wird. Der Beweislauf schreibt es mit. */
export const POLICY_RPC_TOOL = 'get_code_snippet';

/**
 * Was die Policy zum LLM sagt.
 *
 * `absent` deckt zwei Faelle ab, die fuer die Entscheidung dasselbe bedeuten:
 * es gibt keine Datei, oder es gibt eine ohne `llm`-Schluessel. Beide heissen
 * "dieses Projekt hat sich nicht geaeussert".
 */
export type PolicyVerdict = 'deny' | 'allow' | 'absent' | 'unreadable';

/** Was ueber die Policy dieses Projekts bekannt ist. */
export interface PolicyReading {
    verdict: PolicyVerdict;
    /** Der Pfad, um den es geht. Immer gesetzt, damit jeder Satz ihn nennen kann. */
    path: string;
    /** Der Grund, wenn `verdict` nicht aus einem sauberen Wert entstanden ist. */
    detail: string;
}

/** Ob dieses Urteil das LLM sperrt, egal was der Leser eingestellt hat. */
export function blocksLlm(verdict: PolicyVerdict): boolean {
    return verdict === 'deny' || verdict === 'unreadable';
}

/**
 * Den Inhalt der Policy deuten.
 *
 * Getrennt vom Holen, damit die Regel ohne Server pruefbar ist. Erkannt werden
 * genau zwei Werte; alles andere unter `llm` ist eine Aussage, die dieses
 * Programm nicht versteht, und eine unverstandene Aussage in einer
 * Steuerungsdatei wird nicht als Erlaubnis gelesen.
 */
export function readPolicySource(source: string): PolicyReading {
    let parsed: unknown;
    try {
        parsed = JSON.parse(source);
    } catch (error) {
        return {
            verdict: 'unreadable',
            path: POLICY_PATH,
            detail: `die Datei ist kein lesbares JSON: ${error instanceof Error ? error.message : String(error)}`,
        };
    }
    if (typeof parsed !== 'object' || parsed === null || Array.isArray(parsed)) {
        return { verdict: 'unreadable', path: POLICY_PATH, detail: 'die Datei enthaelt kein JSON-Objekt' };
    }
    const value = (parsed as Record<string, unknown>)['llm'];
    if (value === undefined) {
        return { verdict: 'absent', path: POLICY_PATH, detail: '' };
    }
    if (value === 'deny') {
        return { verdict: 'deny', path: POLICY_PATH, detail: '' };
    }
    if (value === 'allow') {
        return { verdict: 'allow', path: POLICY_PATH, detail: '' };
    }
    return {
        verdict: 'unreadable',
        path: POLICY_PATH,
        detail: `"llm" traegt den Wert ${JSON.stringify(value)}, und dieses Programm kennt nur "allow" und "deny"`,
    };
}

/**
 * Die Policy des indizierten Projekts holen und deuten.
 *
 * Eine Datei, die der Index nicht fuehrt, ist keine Policy und kein Fehler:
 * `FileNotReadableError` heisst hier "dieses Projekt hat keine", und dann gilt
 * die Praeferenz. Ein Ausfall der Engine dagegen ist ein Ausfall der Engine und
 * kein Freibrief; er endet in `unreadable`, weil niemand sagen kann, ob dort
 * eine Sperre liegt.
 */
export async function readLlmPolicy(
    client: RpcIntelligenceClient,
    project: string,
): Promise<PolicyReading> {
    if (project.length === 0) {
        return { verdict: 'absent', path: POLICY_PATH, detail: 'kein Projekt gewaehlt' };
    }
    try {
        const document = await loadFileDocument(client, project, POLICY_PATH);
        return readPolicySource(document.source);
    } catch (error) {
        if (error instanceof FileNotReadableError) {
            return { verdict: 'absent', path: POLICY_PATH, detail: '' };
        }
        return {
            verdict: 'unreadable',
            path: POLICY_PATH,
            detail: `die Policy war ueber ${POLICY_RPC_TOOL} nicht zu holen: `
                + `${error instanceof Error ? error.message : String(error)}`,
        };
    }
}
