/**
 * Vom Navigationsziel des Twins zurueck zu Datei und Zeile, die der Reader
 * oeffnen kann.
 *
 * Der Twin traegt Ziele als `SymbolRef`: eine absolute URI plus eine
 * 0-basierte Editor-Spanne. Der Reader dieses Projekts kennt keine URIs, er
 * kennt workspace-relative Pfade, so wie der Baum sie nennt, und 1-basierte
 * Zeilen, so wie der Graph sie fuehrt. Diese Datei ist die eine Stelle, an der
 * zwischen beiden umgerechnet wird.
 *
 * **Warum es einen erfundenen Wurzelpfad gibt.** Im Browser gibt es kein
 * Dateisystem und keinen Arbeitsordner. Der IR-Builder normalisiert trotzdem
 * jede Datei-Angabe der Engine zu einer absoluten URI, weil das Referenzprojekt
 * das tut und ein abweichender IR-Bau die stille Abweichung waere, gegen die
 * dieser Port geschrieben ist. Also bekommt er ein Praefix, das nichts
 * behauptet: `/workspace` ist kein Ort auf dieser Maschine und gibt auch nicht
 * vor, einer zu sein. Es geht hinein und kommt hier wieder heraus, und die
 * Identitaet des Projekts reist getrennt davon als `projectName` mit.
 */

import { toFileUri, toWorkspaceRelative } from '../ir/file-uri';
import { toEditorRange, toGraphLine } from '../core/positions';
import type { CodeAtlasSymbolKind, SymbolRef } from '../core/focus-protocol';

/**
 * Das Praefix, unter dem der IR-Builder die Dateien dieses Projekts fuehrt.
 *
 * Kein Pfad dieser Maschine, sondern ein Platzhalter, der ueberall gleich
 * lautet. Ein echter Pfad waere hier eine Behauptung ueber einen Rechner, den
 * die Oberflaeche nie sieht.
 */
export const ATLAS_WORKSPACE_ROOT = '/workspace';

/** Wohin ein Klick fuehrt: Datei wie im Baum, Zeile wie im Graphen. */
export interface TwinLocation {
    /** Workspace-relativer Pfad, oder leer, wenn das Ziel keinen hat. */
    path: string;
    /** 1-basierte Zeile. Nie kleiner als 1. */
    line: number;
}

/** Der workspace-relative Pfad hinter einer URI aus dem Twin. */
export function workspacePathOf(uri: string | undefined): string {
    return toWorkspaceRelative(ATLAS_WORKSPACE_ROOT, uri);
}

/**
 * Datei und Zeile eines Twin-Ziels.
 *
 * Die Zeile kommt aus `selectionRange`, wenn es eine gibt, sonst aus `range`:
 * `refAt` setzt beide auf dieselbe Zeile, und ein aufgeloestes Symbol traegt in
 * `selectionRange` die Zeile seines Namens, die naeher an dem liegt, was ein
 * Leser sehen will, als der Anfang seiner ganzen Deklaration.
 */
export function twinLocationOf(target: SymbolRef): TwinLocation {
    const editorLine = (target.selectionRange ?? target.range).start.line;
    return { path: workspacePathOf(target.uri), line: toGraphLine(editorLine) };
}

/**
 * Eine Deklaration, so wie Index und Layout sie melden.
 *
 * Genau die Felder, die eine Suchzeile und ein Layout-Knoten tragen, und
 * keines mehr. Zeilen sind 1-basiert wie im Graphen.
 */
export interface DeclarationTarget {
    name: string;
    qualifiedName?: string | undefined;
    kind: CodeAtlasSymbolKind;
    /** Workspace-relativer Pfad. Fehlt er, gibt es kein Ziel. */
    filePath?: string | undefined;
    startLine?: number | undefined;
    endLine?: number | undefined;
}

/**
 * Aus einer gemeldeten Deklaration ein Ziel machen, dem der Twin folgen kann.
 *
 * Die Gegenrichtung zu {@link twinLocationOf}, und sie steht aus demselben
 * Grund hier: es soll eine Stelle geben, an der zwischen der Sprache des
 * Graphen (relativer Pfad, 1-basierte Zeile) und der des Twins (URI,
 * 0-basierte Spanne) umgerechnet wird.
 *
 * **Ohne Datei gibt es kein Ziel.** Ein Ziel mit erfundener URI waere ein
 * Klick, der in einer Datei landet, die es nicht gibt.
 *
 * **`nodeId` bleibt leer, und das ist Absicht.** Das ganze Produkt liest
 * dessen Anwesenheit als "der Index kennt dieses Symbol als Knoten"; eine
 * Suchzeile und ein Layout-Punkt behaupten das nicht, sie nennen eine Stelle.
 * Die Fakten holt der Twin ohnehin erst aus der Aufloesung in der Zieldatei.
 *
 * **Die Zeile ist der Anfang der Deklaration.** Fehlt sie, ist es Zeile 1: der
 * Anfang der Datei ist eine sichtbare Naeherung, eine geratene Mitte waere
 * eine unsichtbare.
 */
export function twinTargetOf(target: DeclarationTarget): SymbolRef | undefined {
    const filePath = target.filePath;
    if (filePath === undefined || filePath.length === 0) {
        return undefined;
    }
    const uri = toFileUri(ATLAS_WORKSPACE_ROOT, filePath);
    if (uri === undefined) {
        return undefined;
    }
    const startLine = target.startLine !== undefined && target.startLine > 0 ? target.startLine : 1;
    const endLine =
        target.endLine !== undefined && target.endLine >= startLine ? target.endLine : startLine;
    return {
        name: target.name,
        qualifiedName: target.qualifiedName,
        kind: target.kind,
        uri,
        range: toEditorRange(startLine, endLine),
        selectionRange: toEditorRange(startLine, startLine),
    };
}
