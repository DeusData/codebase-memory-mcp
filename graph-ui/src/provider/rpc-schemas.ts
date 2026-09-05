/*
 * Herkunft: portiert am 2026-08-28 aus CodeAtlasIDE,
 * /Users/bernhard/Desktop/CodeAtlasIDE/theia-extensions/codeatlas-intelligence/src/node/client/schemas.ts
 * Gleicher Urheber (Bernhard Jackiewicz). Uebernommen wurden die Formen der
 * Engine-Antworten und die Zellen-Koerzierung, weil der portierte Provider
 * genau darauf steht: jede Zahl kommt als Zeichenkette, und jede Umrechnung
 * geht durch eine Stelle statt durch ein Number() an der Aufrufstelle.
 *
 * Zwei Aenderungen gegenueber dem Original, beide begruendet:
 *
 * 1. **Kein zod.** Das Referenzprojekt validiert mit zod; hier stehen
 *    handgeschriebene Leser. Nicht aus Sparsamkeit: dieser Code laeuft im
 *    Browser und das Projekt zieht in diesem Zyklus keine neue Abhaengigkeit.
 *    Die Leser sind dieselbe Art Vertrag wie die Schemas, nur ohne Bibliothek:
 *    ein fehlender Schluessel wird zum dokumentierten Default, ein unbekannter
 *    faellt weg statt den Aufruf zu versenken.
 * 2. **Zwei Drahtformen statt einer.** Die 0.9.0-CLI lieferte Tabellen als
 *    Liste von Objekten. Der Server aus PR 1860 liefert dieselbe Tabelle als
 *    `{cols, rows}` und gruppierte Listen als `{cols, groups}` mit einem
 *    `qn_prefix` je Gruppe. Beide werden hier auf die Objektform gebracht, die
 *    der portierte Provider schon liest, damit die Aenderung des Servers genau
 *    hier endet und nicht durch die Produktlogik wandert.
 */

/**
 * Der Leerwert einer kompakten Zelle.
 *
 * Die kompakte Zeilenform schreibt fuer eine nicht gesetzte Eigenschaft einen
 * nackten Bindestrich, wo die JSON-Form derselben Abfrage `""` schreibt. Das
 * ist gegen den gebauten Server geprueft: dieselbe RAISES-Abfrage liefert
 * `ValidationError src/util/validate.ts "4" -` im Text und
 * `["ValidationError","src/util/validate.ts","4",""]` mit `format: "json"`.
 *
 * Der Preis der Uebersetzung ist benannt: eine Zelle, deren Wert wirklich aus
 * einem Bindestrich besteht, ist von einem Leerwert nicht zu unterscheiden.
 * Fuer Namen, Pfade und Zeilennummern kommt das nicht vor, und die Alternative
 * waere, jeden Leerwert als die Zeichenkette "-" durch das Produkt zu tragen,
 * wo er als Dateiname auf dem Bildschirm landet.
 */
export const EMPTY_CELL = '-';

/** Eine kompakte Zelle als Wert: der Leermarker wird zur leeren Zeichenkette. */
export function normalizeCell(cell: string): string {
    return cell === EMPTY_CELL ? '' : cell;
}

function isRecord(value: unknown): value is Record<string, unknown> {
    return typeof value === 'object' && value !== null && !Array.isArray(value);
}

/** Eine tabellarische Zelle, wie die Engine sie schreibt: immer Text, manchmal leer. */
export function toCell(value: unknown): string {
    if (value === null || value === undefined) {
        return '';
    }
    return typeof value === 'string' ? value : String(value);
}

/** Eine Zelle als Zahl, oder undefined wenn sie leer oder keine Zahl ist. */
export function toOptionalNumber(cell: unknown): number | undefined {
    if (cell === null || cell === undefined || cell === '') {
        return undefined;
    }
    const parsed = typeof cell === 'number' ? cell : Number(cell);
    return Number.isFinite(parsed) ? parsed : undefined;
}

/** Eine Zelle als Wahrheitswert. Die Engine schreibt `"true"` und `"false"`. */
export function toBoolean(cell: unknown): boolean {
    return cell === true || cell === 'true';
}

function toText(value: unknown): string | undefined {
    return typeof value === 'string' && value.length > 0 ? value : undefined;
}

function toStringList(value: unknown): string[] {
    return Array.isArray(value) ? value.filter((entry): entry is string => typeof entry === 'string') : [];
}

/**
 * Eine Tabelle als eine Zeile je Objekt, egal in welcher der drei Drahtformen
 * sie ankommt.
 *
 * - Liste von Objekten: die Form der 0.9.0-CLI, unveraendert durchgereicht.
 * - `{cols, rows}`: die Form des Servers aus PR 1860 fuer eine flache Tabelle.
 * - `{cols, groups}`: dieselbe Tabelle gruppiert. Der Gruppenkopf traegt
 *   `qn_prefix` und manchmal `file`; beide werden an jede Zeile der Gruppe
 *   geheftet, weil sie sonst beim Abflachen verloren gingen und der Aufrufer
 *   den qualifizierten Namen nicht mehr zusammensetzen koennte.
 */
export function tableRows(value: unknown): Record<string, unknown>[] {
    if (Array.isArray(value)) {
        return value.filter(isRecord);
    }
    if (!isRecord(value)) {
        return [];
    }
    const cols = toStringList(value['cols']);
    const zip = (row: unknown, extra: Record<string, unknown> = {}): Record<string, unknown> => {
        const out: Record<string, unknown> = { ...extra };
        if (Array.isArray(row)) {
            cols.forEach((column, index) => {
                out[column] = row[index];
            });
        } else if (isRecord(row)) {
            Object.assign(out, row);
        }
        return out;
    };
    const groups = value['groups'];
    if (Array.isArray(groups)) {
        const out: Record<string, unknown>[] = [];
        for (const group of groups) {
            if (!isRecord(group)) {
                continue;
            }
            const extra: Record<string, unknown> = {};
            const prefix = toText(group['qn_prefix']);
            const file = toText(group['file']);
            if (prefix !== undefined) {
                extra['qn_prefix'] = prefix;
            }
            if (file !== undefined) {
                extra['file'] = file;
            }
            for (const row of Array.isArray(group['rows']) ? group['rows'] : []) {
                out.push(zip(row, extra));
            }
        }
        return out;
    }
    const rows = value['rows'];
    return Array.isArray(rows) ? rows.map((row) => zip(row)) : [];
}

/**
 * Der qualifizierte Name einer gruppierten Zeile.
 *
 * Der Server sagt es selbst im Kopf der Antwort: `qn = group prefix + "." +
 * name`. Ohne Prefix gibt es keinen, und dann ist die ehrliche Antwort
 * undefined statt der blosse Name, der in einem anderen Modul dasselbe heissen
 * kann.
 */
export function qualifiedNameOf(row: Record<string, unknown>): string | undefined {
    const direct = toText(row['qualified_name']) ?? toText(row['qn']);
    if (direct !== undefined) {
        return direct;
    }
    const prefix = toText(row['qn_prefix']);
    const name = toText(row['name']);
    return prefix !== undefined && name !== undefined ? `${prefix}.${name}` : undefined;
}

/** Tabellarische Antwort: benannte Spalten plus Zeilen aus lauter Zeichenketten. */
export interface QueryGraphResult {
    columns: string[];
    rows: string[][];
    total?: number;
}

/** Ein Kandidat, wenn ein blosser Name auf mehr als ein Symbol passte. */
export interface PathSuggestion {
    qualified_name: string;
    name: string;
    label?: string;
    file_path?: string;
}

/** Ein Symbol, das ein Pfadlauf erreicht hat. */
export interface PathHop {
    name: string;
    qualified_name?: string;
    hop?: number;
}

/** Der Lauf konnte nicht starten: der Name passte auf mehrere Symbole. */
export interface PathAmbiguous {
    status: 'ambiguous';
    message?: string;
    suggestions: PathSuggestion[];
}

/**
 * Der Lauf lief. Beide Nachbarlisten sind optional: nach einer Richtung
 * gefragt, fehlt die andere ganz, statt leer zu sein.
 */
export interface PathWalk {
    function: string;
    direction?: string;
    callees?: PathHop[];
    callers?: PathHop[];
}

export type PathResult = PathAmbiguous | PathWalk;

/** True, wenn die Pfadantwort die Ablehnung wegen Mehrdeutigkeit ist. */
export function isAmbiguousPath(result: PathResult): result is PathAmbiguous {
    return (result as PathAmbiguous).status === 'ambiguous';
}

export interface ProjectEntry {
    name: string;
    root_path?: string;
    nodes?: number;
    edges?: number;
}

export interface ListProjectsResult {
    projects: ProjectEntry[];
}

export interface IndexStatusResult {
    project?: string;
    nodes?: number;
    edges?: number;
    status?: string;
    root_path?: string;
}

export interface IndexRepositoryResult {
    project?: string;
    nodes?: number;
    edges?: number;
    status?: string;
    skipped_count?: number;
}

export interface ArchitectureResult {
    project?: string;
    total_nodes?: number;
    total_edges?: number;
    node_labels: { label: string; count?: number }[];
    edge_types: { type: string; count?: number }[];
    languages: { language: string; file_count?: number }[];
    packages: { name: string; node_count?: number; fan_in?: number; fan_out?: number }[];
    entry_points: { name: string; qualified_name?: string; file?: string }[];
    routes: {
        method?: string;
        path?: string;
        handler?: string;
        file?: string;
        file_path?: string;
        line?: number;
    }[];
    hotspots: { name: string; qualified_name?: string; file_path?: string; fan_in?: number }[];
    layers: { name?: string; layer?: string; reason?: string }[];
    boundaries: { from?: string; to?: string; call_count?: number }[];
    clusters: {
        id?: string | number;
        label?: string;
        members?: number;
        cohesion?: number;
        top_nodes: string[];
        packages: string[];
    }[];
    file_tree: { path: string; type?: string; children?: number }[];
}

/** Ein Symbol, das eine Aenderungsmenge nennt. */
export interface DetectedSymbol {
    name?: string;
    label?: string;
    /** Workspace-relativer Pfad. Der Schluessel, den 0.9.0 wirklich schreibt. */
    file?: string;
    /** Die Schreibweise jeder anderen Antwort. */
    file_path?: string;
    qualified_name?: string;
    /**
     * Wie viele Aufrufschritte dieses Symbol von der Aenderung entfernt ist.
     *
     * Der Server aus PR 1860 schreibt die Spalte, und sie ist keine Zierde: er
     * listet unter `impacted` NICHT die geaenderten Symbole, sondern deren
     * transitive Aufrufer, und `hop` sagt, in welchem Schritt jedes davon
     * erreicht wurde. Wer die Spalte wegwirft, macht aus einem Aufrufer ein
     * geaendertes Symbol. Fehlt sie, ist es eine Antwort ohne diese Aussage;
     * der Provider behandelt sie dann wie 0.9.0 es meinte.
     */
    hop?: number;
}

export interface DetectChangesResult {
    changed_files: string[];
    changed_count?: number;
    impacted_symbols: DetectedSymbol[];
    depth?: number;
    /**
     * Wie viele Deklarationen der Server als Ausgangspunkt genommen hat.
     *
     * Er nennt nur die Zahl, nie die Namen: die Auswahl ist auf die geaenderten
     * Hunks eingeschraenkt, also ist sie schaerfer als "alles, was in der Datei
     * steht". Die Zahl bleibt trotzdem wertvoll, weil sie die einzige Aussage
     * darueber ist, wie viel von einer geaenderten Datei die Analyse wirklich
     * angefasst hat.
     */
    seed_symbols?: number;
    /** True, wenn der Aufrufer-Lauf des Servers an seiner Decke endete. */
    truncated?: boolean;
}

export interface CodeSnippetResult {
    name?: string;
    qualified_name?: string;
    label?: string;
    file_path?: string;
    start_line?: number;
    end_line?: number;
    source: string;
    /**
     * Der Server hat den Schnipsel gekappt und sagt es selbst.
     *
     * Ein Modul-Knoten umspannt seine ganze Datei, und genau dagegen hat der
     * Server einen Deckel: `MCP_SNIPPET_MAX_LINES` (500, cbm/src/mcp/mcp.c).
     * Wird er erreicht, setzt der Server `source_clipped` und nennt in
     * `clipped_at_lines` den Deckel. Ohne diese beiden Felder waere ein
     * gekappter Schnipsel von einer vollstaendigen Datei nicht zu
     * unterscheiden, und der Reader wuerde 500 Zeilen als "die Datei" zeigen.
     */
    source_clipped?: boolean;
    /** Der Deckel, bei dem gekappt wurde. Nur gesetzt, wenn gekappt wurde. */
    clipped_at_lines?: number;
}

export interface SearchGraphHit {
    name: string;
    qualified_name?: string;
    label?: string;
    file_path?: string;
    start_line?: number;
    is_test?: boolean;
    is_exported?: boolean;
}

export interface SearchGraphResult {
    total?: number;
    results: SearchGraphHit[];
}

// -------------------------------------------------------------- Leser -------

/**
 * Die Leser unten sind absichtlich nachsichtig gegenueber dem, was sie nicht
 * kennen, und streng gegenueber dem, was sie behaupten: ein fehlender
 * Schluessel wird zu einem dokumentierten Default, ein unbekannter faellt weg.
 * Was nie passiert, ist dass ein fehlender Wert zu einem erfundenen wird.
 */

function record(value: unknown): Record<string, unknown> {
    return isRecord(value) ? value : {};
}

export function readListProjects(value: unknown): ListProjectsResult {
    const raw = record(value);
    const projects: ProjectEntry[] = [];
    for (const entry of tableRows(raw['projects'])) {
        const name = toText(entry['name']);
        if (name === undefined) {
            continue;
        }
        projects.push({
            name,
            root_path: toText(entry['root_path']),
            nodes: toOptionalNumber(entry['nodes']),
            edges: toOptionalNumber(entry['edges']),
        });
    }
    return { projects };
}

export function readIndexStatus(value: unknown): IndexStatusResult {
    const raw = record(value);
    return {
        project: toText(raw['project']),
        nodes: toOptionalNumber(raw['nodes']),
        edges: toOptionalNumber(raw['edges']),
        status: toText(raw['status']),
        root_path: toText(raw['root_path']),
    };
}

export function readCodeSnippet(value: unknown): CodeSnippetResult {
    const raw = record(value);
    const result: CodeSnippetResult = {
        name: toText(raw['name']),
        qualified_name: toText(raw['qualified_name']),
        label: toText(raw['label']),
        file_path: toText(raw['file_path']),
        start_line: toOptionalNumber(raw['start_line']),
        end_line: toOptionalNumber(raw['end_line']),
        source: typeof raw['source'] === 'string' ? (raw['source'] as string) : '',
    };
    // Nur uebernehmen, wenn der Server es sagt. `false` waere hier die Aussage
    // "nicht gekappt", und die hat ein Server, der das Feld gar nicht kennt,
    // nicht gemacht.
    if (raw['source_clipped'] !== undefined) {
        result.source_clipped = toBoolean(raw['source_clipped']);
    }
    const clippedAt = toOptionalNumber(raw['clipped_at_lines']);
    if (clippedAt !== undefined) {
        result.clipped_at_lines = clippedAt;
    }
    return result;
}

/**
 * Die Pfadantwort, beide Formen.
 *
 * Der Server liefert die Ablehnung als JSON-Objekt und den gelaufenen Pfad mit
 * `format: "json"` als zwei gruppierte Tabellen. Die Gruppen werden hier zu
 * Hops abgeflacht, denn der qualifizierte Name steckt im Gruppenkopf und der
 * Aufrufer haette ihn sonst nicht mehr.
 */
export function readPathResult(value: unknown): PathResult {
    const raw = record(value);
    if (raw['status'] === 'ambiguous') {
        const suggestions: PathSuggestion[] = [];
        for (const entry of tableRows(raw['suggestions'])) {
            const qualifiedName = qualifiedNameOf(entry);
            const name = toText(entry['name']);
            if (qualifiedName === undefined || name === undefined) {
                continue;
            }
            suggestions.push({
                qualified_name: qualifiedName,
                name,
                label: toText(entry['label']),
                file_path: toText(entry['file_path']),
            });
        }
        return { status: 'ambiguous', message: toText(raw['message']), suggestions };
    }

    const hopsOf = (key: string): PathHop[] | undefined => {
        if (raw[key] === undefined) {
            return undefined;
        }
        const hops: PathHop[] = [];
        for (const entry of tableRows(raw[key])) {
            const name = toText(entry['name']);
            if (name === undefined) {
                continue;
            }
            hops.push({
                name,
                qualified_name: qualifiedNameOf(entry),
                hop: toOptionalNumber(entry['hop']),
            });
        }
        return hops;
    };

    const walk: PathWalk = { function: toText(raw['function']) ?? '', direction: toText(raw['direction']) };
    const callees = hopsOf('callees');
    const callers = hopsOf('callers');
    if (callees !== undefined) {
        walk.callees = callees;
    }
    if (callers !== undefined) {
        walk.callers = callers;
    }
    return walk;
}

/**
 * Die Suchantwort.
 *
 * Der Server rankt mit BM25 und antwortet mit `qn label file lines rank`. Zwei
 * Spalten der 0.9.0-Antwort fehlen darin: `is_test` und `is_exported`. Sie
 * bleiben undefined statt false, weil die Suche sie nicht beantwortet hat und
 * false die Antwort "nein" waere. `lines` ist eine Spanne der Form `23-36`;
 * genommen wird ihr Anfang, denn das ist die Deklarationszeile.
 */
export function readSearchRows(rows: Record<string, string>[], total?: number): SearchGraphResult {
    const results: SearchGraphHit[] = [];
    for (const row of rows) {
        const qualifiedName = qualifiedNameOf(row);
        const name = toText(row['name']) ?? qualifiedName?.split('.').pop();
        if (name === undefined) {
            continue;
        }
        const hit: SearchGraphHit = {
            name,
            qualified_name: qualifiedName,
            label: toText(row['label']),
            file_path: toText(row['file']) ?? toText(row['file_path']),
            start_line: toOptionalNumber((toText(row['lines']) ?? '').split('-')[0]),
        };
        if (row['is_test'] !== undefined) {
            hit.is_test = toBoolean(row['is_test']);
        }
        if (row['is_exported'] !== undefined) {
            hit.is_exported = toBoolean(row['is_exported']);
        }
        results.push(hit);
    }
    return { total, results };
}

/**
 * Die Aenderungsmenge.
 *
 * Der Server nennt die betroffenen Symbole `impacted`, die CLI nannte sie
 * `impacted_symbols`; beide Schreibweisen werden gelesen. Was das Original
 * ueber diese Liste festgehalten hat, gilt weiter: eine Zeile traegt einen
 * Anzeigenamen, ein Label und eine Datei, die Datei unter dem Schluessel
 * `file`, und die Datei selbst taucht als Symbol mit dem Label `Module` auf.
 */
export function readDetectChanges(value: unknown): DetectChangesResult {
    const raw = record(value);
    const impacted = raw['impacted_symbols'] ?? raw['impacted'];
    // Der Anzeigename, wenn die Zeile nur einen qualifizierten Namen traegt:
    // dessen letztes Segment. Dieselbe Zerlegung wie in readArchitecture, und
    // aus demselben Grund keine Erfindung: der Server beschreibt sie im Kopf
    // seiner eigenen Antworten. Ohne sie stuende in einer Zeile der
    // Aenderungsansicht der ganze Punktpfad, wo ein Funktionsname hingehoert.
    const displayName = (entry: Record<string, unknown>): string | undefined => {
        const written = toText(entry['name']);
        if (written !== undefined && written.length > 0) {
            return written;
        }
        const qualified = qualifiedNameOf(entry);
        return qualified === undefined ? undefined : (qualified.split('.').pop() ?? qualified);
    };
    const symbols: DetectedSymbol[] = tableRows(impacted).map((entry) => ({
        name: displayName(entry),
        label: toText(entry['label']),
        file: toText(entry['file']),
        file_path: toText(entry['file_path']),
        qualified_name: qualifiedNameOf(entry),
        hop: toOptionalNumber(entry['hop']),
    }));
    return {
        changed_files: toStringList(raw['changed_files']),
        changed_count: toOptionalNumber(raw['changed_count']),
        impacted_symbols: symbols,
        depth: toOptionalNumber(raw['depth']),
        seed_symbols: toOptionalNumber(raw['seed_symbols']),
        truncated: raw['truncated'] === true,
    };
}

/**
 * Die Projektuebersicht.
 *
 * Der Server aus PR 1860 schickt jeden Aspekt als `{cols, rows}` und
 * buchstabiert drei Spalten anders als die CLI: `files` statt `file_count`,
 * `nodes` statt `node_count`, `calls` statt `call_count`. Die Umbenennung
 * passiert hier und nur hier. Die Einstiegspunkte und die Hotspots nennen nur
 * noch einen qualifizierten Namen; der Anzeigename ist dessen letztes
 * Segment, was keine Erfindung ist, sondern die Zerlegung, die der Server im
 * Kopf seiner eigenen Antworten beschreibt.
 */
export function readArchitecture(value: unknown): ArchitectureResult {
    const raw = record(value);
    const lastSegment = (qualifiedName: string | undefined): string =>
        qualifiedName === undefined ? '' : (qualifiedName.split('.').pop() ?? qualifiedName);

    return {
        project: toText(raw['project']),
        total_nodes: toOptionalNumber(raw['total_nodes']),
        total_edges: toOptionalNumber(raw['total_edges']),
        node_labels: tableRows(raw['node_labels'])
            .map((row) => ({ label: toText(row['label']) ?? '', count: toOptionalNumber(row['count']) }))
            .filter((entry) => entry.label.length > 0),
        edge_types: tableRows(raw['edge_types'])
            .map((row) => ({ type: toText(row['type']) ?? '', count: toOptionalNumber(row['count']) }))
            .filter((entry) => entry.type.length > 0),
        languages: tableRows(raw['languages'])
            .map((row) => ({
                language: toText(row['language']) ?? '',
                file_count: toOptionalNumber(row['file_count'] ?? row['files']),
            }))
            .filter((entry) => entry.language.length > 0),
        packages: tableRows(raw['packages'])
            .map((row) => ({
                name: toText(row['name']) ?? '',
                node_count: toOptionalNumber(row['node_count'] ?? row['nodes']),
                fan_in: toOptionalNumber(row['fan_in']),
                fan_out: toOptionalNumber(row['fan_out']),
            }))
            .filter((entry) => entry.name.length > 0),
        entry_points: tableRows(raw['entry_points']).map((row) => {
            const qualifiedName = qualifiedNameOf(row);
            return {
                name: toText(row['name']) ?? lastSegment(qualifiedName),
                qualified_name: qualifiedName,
                file: toText(row['file']) ?? toText(row['file_path']),
            };
        }),
        routes: tableRows(raw['routes']).map((row) => ({
            method: toText(row['method']),
            path: toText(row['path']),
            handler: toText(row['handler']),
            file: toText(row['file']),
            file_path: toText(row['file_path']),
            line: toOptionalNumber(row['line']),
        })),
        hotspots: tableRows(raw['hotspots']).map((row) => {
            const qualifiedName = qualifiedNameOf(row);
            return {
                name: toText(row['name']) ?? lastSegment(qualifiedName),
                qualified_name: qualifiedName,
                file_path: toText(row['file_path']) ?? toText(row['file']),
                fan_in: toOptionalNumber(row['fan_in']),
            };
        }),
        layers: tableRows(raw['layers']).map((row) => ({
            name: toText(row['name']),
            layer: toText(row['layer']),
            reason: toText(row['reason']),
        })),
        boundaries: tableRows(raw['boundaries']).map((row) => ({
            from: toText(row['from']),
            to: toText(row['to']),
            call_count: toOptionalNumber(row['call_count'] ?? row['calls']),
        })),
        clusters: tableRows(raw['clusters']).map((row) => ({
            id: typeof row['id'] === 'number' ? row['id'] : toText(row['id']),
            label: toText(row['label']),
            members: toOptionalNumber(row['members']),
            cohesion: toOptionalNumber(row['cohesion']),
            top_nodes: toStringList(row['top_nodes']),
            packages: toStringList(row['packages']),
        })),
        file_tree: tableRows(raw['file_tree'])
            .map((row) => ({
                path: toText(row['path']) ?? '',
                type: toText(row['type']),
                children: toOptionalNumber(row['children']),
            }))
            .filter((entry) => entry.path.length > 0),
    };
}
