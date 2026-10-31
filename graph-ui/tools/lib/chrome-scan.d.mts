/**
 * Die Typen des Chrome-Scans, damit ihn ein TypeScript-Test aufrufen kann,
 * ohne dass der Scan selbst nach TypeScript uebersetzt werden muss.
 *
 * Der Scan ist ein Werkzeug und lebt unter tools/, wo auch das Netz-Deny-Gate
 * und die Beweislaeufe liegen. Ihn nach src/ zu verschieben, damit ein Test ihn
 * importieren kann, hiesse ein Werkzeug in das Produkt zu legen; eine
 * Deklarationsdatei daneben kostet nichts und sagt dasselbe.
 */

export interface ChromeScanFinding {
    /** Pfad relativ zur Projektwurzel. */
    file: string;
    line: number;
    column: number;
    /** Welche der fuenf Regeln gegriffen hat, etwa `jsx-text` oder `attribute:title`. */
    rule: string;
    /** Der gefundene Text, auf 120 Zeichen gekuerzt. */
    text: string;
}

export interface ChromeScanException {
    rule: string;
    reason: string;
}

export declare const ROOT: string;
export declare const CHROME_FILES: string[];
export declare const SCAN_WHITELIST: ChromeScanException[];
export declare function scanFile(relativePath: string, root?: string): ChromeScanFinding[];
export declare function scanChrome(files?: string[], root?: string): ChromeScanFinding[];
