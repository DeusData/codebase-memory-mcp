/**
 * Der Datei-Baum, wie ihn der Explorer zeigt: die VEREINIGUNG aus dem Graphen
 * und den Coverage-Listen des Servers.
 *
 * `/api/tree?project=X&path=Y` antwortet mit genau einer Ebene: den direkten
 * Kindern von Y, jedes mit `name`, `path`, `kind` ("dir" oder "file") und den
 * Zahlen `symbols` und `files`. Der Baum kommt also stueckweise, eine Ebene je
 * Anfrage, und dieses Modul haelt die Ebenen zusammen und rechnet aus, welche
 * Zeilen davon gerade sichtbar sind.
 *
 * **Der Graph ist nur eine der Quellen.** Bis W4c war er die einzige, und damit
 * zeigte der Explorer genau die Dateien, aus denen Symbole geworden sind. Eine
 * Datei, die der Indexer uebersprungen hat, war unsichtbar, und Unsichtbarkeit
 * ist in einem Explorer die Behauptung, es gebe sie nicht. Seit W4d stehen
 * daneben zwei weitere Quellen, beide vom selben Server:
 *
 * - `index_status` traegt eine Coverage-Beilage: `parse_partial` (indiziert,
 *   aber mit Parse-Fehlern), `skipped` (gar nicht indiziert, mit Grund) und
 *   `not_indexed` (nach Absicht ausgeschlossen, Dateien UND Verzeichnisse).
 *   Jede der drei Listen nennt ihre eigene Zahl und ihr eigenes `truncated`.
 * - `check_index_coverage` mit `scopes: ["."]` listet die Eintraege des
 *   Coverage-Stores selbst, paginiert ueber `scope_offset`, und deckt damit
 *   auch Faelle, die `index_status` kappt.
 *
 * Der Coverage-Store ist eine eigene Tabelle NEBEN dem Graphen (mcp.c: "coverage
 * is metadata ABOUT the graph, stored outside it"). Darum ist die Vereinigung
 * keine Bequemlichkeit, sondern die einzige Form, in der der Explorer beide
 * Wahrheiten gleichzeitig zeigen kann: was indiziert wurde, und was der Index
 * angefasst und liegengelassen hat.
 *
 * Was danach immer noch fehlt, wird nicht verschwiegen: eine Datei, die die
 * Discovery nie gesehen hat, steht in keiner der drei Quellen. Die Legende im
 * Explorer-Fuss sagt das mit eigenen Worten, statt den Baum vollstaendig
 * aussehen zu lassen.
 *
 * Zwei Eigenheiten des Graph-Baums bleiben, an einem laufenden Server
 * abgelesen und hier ausdruecklich behandelt, statt sie im Explorer als
 * Geisterzeilen zu zeigen:
 *
 * 1. **Ordner erscheinen doppelt.** Der Server gruppiert alle Knoten nach
 *    `file_path`. Ein Folder-Knoten traegt als `file_path` den Ordnerpfad
 *    selbst, also faellt zu `src` ein zweites Kind an, diesmal mit
 *    `kind: "file"`. Zwei Kinder mit demselben Pfad sind aber ein Ding: der
 *    Ordner. Sie werden zusammengelegt, der Ordner gewinnt, und wie oft das
 *    passiert ist, steht in `foldedDuplicates`.
 * 2. **`{}` ist kein Pfad.** Die Project- und Branch-Knoten des Projekts
 *    tragen `{}` als `file_path`, und der Server macht daraus brav ein Kind
 *    namens `{}`. Eine Datei dieses Namens gibt es nicht. Solche Kinder fallen
 *    weg und werden in `droppedNonPaths` gezaehlt, damit der Explorer sagen
 *    kann, dass er etwas weggelassen hat, statt es zu verschweigen.
 *
 * Beides ist ein Serverbefund und gehoert auf die Ask-Liste, nicht in eine
 * stille Reparatur: gezaehlt wird, was wegfaellt.
 */

/**
 * Wie weit der Index eine Datei erfasst hat.
 *
 * Die fuenf Werte sind genau die Klassen, die der Server meldet, und keine
 * mehr: `indexed` ist die Abwesenheit eines Befundes, die anderen vier kommen
 * aus dem `kind` einer Coverage-Zeile. Ein sechster Wert waere ein Status, den
 * niemand gemeldet hat.
 */
export type CoverageState = 'indexed' | 'partial' | 'skipped' | 'not-indexed' | 'ignored';

/**
 * Die Stufen von "ganz da" nach "am unerklaertesten weg".
 *
 * Die Reihenfolge ist eine Aussage und keine Geschmacksfrage. `partial` steht
 * vorn, weil die Datei im Graphen steht und nur Teile fehlen. Danach kommen die
 * beiden erklaerten Abwesenheiten: `ignored` (der Coverage-Store fuehrt sie als
 * ignoriert) und `not-indexed` (nach Absicht ausgeschlossen, mit Regel als
 * Grund). Ganz hinten steht `skipped`, denn dort hat der Indexer die Datei
 * angefasst und aufgegeben: eine unerklaerte Luecke ist fuer einen Leser
 * schlimmer als eine erklaerte.
 */
export const COVERAGE_ORDER: readonly CoverageState[] = [
    'indexed',
    'partial',
    'ignored',
    'not-indexed',
    'skipped',
];

/** Der Rang einer Stufe. Groesser heisst schlechter erfasst. */
export function coverageRank(state: CoverageState): number {
    const index = COVERAGE_ORDER.indexOf(state);
    return index === -1 ? 0 : index;
}

/** Die schlechtere der beiden Stufen. Der Konfliktloeser des Joins. */
export function worstCoverage(left: CoverageState, right: CoverageState): CoverageState {
    return coverageRank(right) > coverageRank(left) ? right : left;
}

/**
 * Das `kind` einer Coverage-Zeile als Stufe.
 *
 * Der Server schreibt `parse_partial`, `not_indexed_dir`, `not_indexed_file`
 * und ansonsten den Namen der Phase, in der er aufgegeben hat (mcp.c,
 * add_coverage_report: alles, was keiner der drei Namen ist, landet unter
 * `skipped`). Genau diese Zuordnung steht hier, damit die Oberflaeche keine
 * eigene erfindet.
 */
export function coverageStateForKind(kind: string): CoverageState {
    if (kind === 'parse_partial') {
        return 'partial';
    }
    if (kind.startsWith('not_indexed')) {
        return 'not-indexed';
    }
    if (kind.startsWith('ignored') || kind.startsWith('gitignored')) {
        return 'ignored';
    }
    return 'skipped';
}

/** Ein Kind einer Baum-Ebene, so wie der Explorer es zeigt. */
export interface TreeChild {
    name: string;
    path: string;
    kind: 'dir' | 'file';
    /** Symbole, die der Graph unter diesem Kind fuehrt. */
    symbols: number;
    /** Dateien, die der Graph unter diesem Kind fuehrt. */
    files: number;
    /** Dateien, die der Index unter diesem Kind ausgelassen hat, wenn er es sagt. */
    missed?: number;
    /**
     * Die Stufe dieser Zeile. Bei Ordnern die schlechteste ihres Teilbaums.
     *
     * Fehlt sie, ist noch nichts gejoint worden. Das ist ausdruecklich nicht
     * dasselbe wie `indexed`: solange die Coverage-Antwort nicht da ist, hat
     * niemand etwas ueber diese Datei gesagt.
     */
    coverage?: CoverageState;
    /** Der Grund des Servers, soweit er einen genannt hat. */
    coverageReason?: string;
    /** Die Quellen, die diesen Pfad genannt haben. Fuer die ehrliche Herkunft. */
    coverageSources?: string[];
}

/** Eine Ebene des Baums, plus was auf dem Weg dorthin weggefallen ist. */
export interface TreeLevel {
    path: string;
    files: number;
    symbols: number;
    children: TreeChild[];
    /** Wie viele Kinder mit einem Ordner desselben Pfades zusammengelegt wurden. */
    foldedDuplicates: number;
    /** Wie viele Kinder keinen Pfad benannt haben und darum weggefallen sind. */
    droppedNonPaths: number;
    /**
     * True, wenn diese Ebene nicht von `/api/tree` kam, sondern beim Join
     * entstanden ist, weil ein Coverage-Eintrag darunter liegt.
     *
     * Der Explorer braucht das, um ein fehlgeschlagenes Nachladen einer solchen
     * Ebene nicht als Baumfehler zu melden: ein Ordner, den nur die
     * Coverage-Listen kennen, hat im Graphen keine Ebene, und das ist der
     * erwartete Fall und kein Ausfall.
     */
    synthetic?: boolean;
}

/** Eine sichtbare Zeile des Explorers. */
export interface TreeRow extends TreeChild {
    /** Verschachtelungstiefe, 0 fuer die oberste Ebene. */
    depth: number;
    /** Nur fuer Ordner: ob die Ebene darunter gerade gezeigt wird. */
    expanded: boolean;
    /** Nur fuer Ordner: ob die Ebene darunter schon geladen ist. */
    loaded: boolean;
}

function isRecord(value: unknown): value is Record<string, unknown> {
    return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function asNumber(value: unknown): number {
    const parsed = typeof value === 'number' ? value : Number(value);
    return Number.isFinite(parsed) ? parsed : 0;
}

/**
 * Ob ein Kind einen Pfad benennt.
 *
 * Die Pruefung ist absichtlich schmal: sie kennt genau die eine Form, die der
 * Server statt eines Pfades schickt (`{}`, das leere JSON-Objekt der
 * Project- und Branch-Knoten), und sonst nichts. Eine breitere Heuristik wuerde
 * frueher oder spaeter eine echte Datei verschlucken, und eine verschluckte
 * Datei ist schlimmer als eine haessliche Zeile.
 */
export function namesAPath(child: { name: string; path: string }): boolean {
    if (child.name.length === 0 || child.path.length === 0) {
        return false;
    }
    return child.name !== '{}' && child.path !== '{}';
}

/**
 * Anzeigeordnung: Ordner zuerst, dann Dateien, beides alphabetisch.
 *
 * Der Server sortiert nach Symbolzahl. Das ist eine sinnvolle Ordnung fuer eine
 * Auswertung und eine schlechte fuer einen Explorer, in dem man eine Datei
 * *suchen* will. Die Ordnung hier ist stabil und haengt an nichts, was sich
 * beim naechsten Indexlauf verschiebt.
 */
export function compareChildren(left: TreeChild, right: TreeChild): number {
    if (left.kind !== right.kind) {
        return left.kind === 'dir' ? -1 : 1;
    }
    return left.name.localeCompare(right.name, 'en');
}

/** Eine Antwort von `/api/tree` lesen: falten, aussortieren, sortieren. */
export function readTreeLevel(payload: unknown): TreeLevel {
    const raw = isRecord(payload) ? payload : {};
    const rawChildren = Array.isArray(raw['children']) ? raw['children'] : [];

    const byPath = new Map<string, TreeChild>();
    let foldedDuplicates = 0;
    let droppedNonPaths = 0;

    for (const entry of rawChildren) {
        if (!isRecord(entry)) {
            continue;
        }
        const name = typeof entry['name'] === 'string' ? entry['name'] : '';
        const path = typeof entry['path'] === 'string' ? entry['path'] : '';
        if (!namesAPath({ name, path })) {
            droppedNonPaths += 1;
            continue;
        }
        const child: TreeChild = {
            name,
            path,
            kind: entry['kind'] === 'dir' ? 'dir' : 'file',
            symbols: asNumber(entry['symbols']),
            files: asNumber(entry['files']),
        };
        const missed = asNumber(entry['missed']);
        if (missed > 0) {
            child.missed = missed;
        }

        const existing = byPath.get(path);
        if (existing === undefined) {
            byPath.set(path, child);
            continue;
        }
        // Derselbe Pfad zweimal: der Ordner ist die Wahrheit, die Zahlen des
        // Ordners auch. Der Folder-Knoten steuert nur sich selbst bei.
        foldedDuplicates += 1;
        if (existing.kind === 'file' && child.kind === 'dir') {
            byPath.set(path, child);
        }
    }

    return {
        path: typeof raw['path'] === 'string' ? raw['path'] : '',
        files: asNumber(raw['files']),
        symbols: asNumber(raw['symbols']),
        children: [...byPath.values()].sort(compareChildren),
        foldedDuplicates,
        droppedNonPaths,
    };
}

/** Der Pfad des Ordners ueber diesem Pfad. Leer heisst Projektwurzel. */
export function parentPath(path: string): string {
    const slash = path.lastIndexOf('/');
    return slash === -1 ? '' : path.slice(0, slash);
}

/**
 * Die sichtbaren Zeilen: die Wurzel-Ebene, und unter jedem aufgeklappten
 * Ordner dessen geladene Ebene.
 *
 * Ein aufgeklappter Ordner ohne geladene Ebene erzeugt keine Zeilen und keinen
 * Platzhalter. Er traegt `loaded: false`, und der Explorer zeigt das am
 * Pfeil-Zeichen, statt eine leere Ebene als "hier ist nichts" auszugeben.
 */
export function flattenTree(
    levels: ReadonlyMap<string, TreeLevel>,
    expanded: ReadonlySet<string>,
    rootPath = '',
): TreeRow[] {
    const rows: TreeRow[] = [];
    const walk = (path: string, depth: number, seen: Set<string>): void => {
        const level = levels.get(path);
        if (level === undefined || seen.has(path)) {
            return;
        }
        seen.add(path);
        for (const child of level.children) {
            const isExpanded = child.kind === 'dir' && expanded.has(child.path);
            rows.push({
                ...child,
                depth,
                expanded: isExpanded,
                loaded: child.kind === 'dir' ? levels.has(child.path) : true,
            });
            if (isExpanded) {
                walk(child.path, depth + 1, seen);
            }
        }
    };
    walk(rootPath, 0, new Set());
    return rows;
}

/** Alle Ordnerpfade einer Ebene, in Anzeigeordnung. */
export function directoryPaths(level: TreeLevel): string[] {
    return level.children.filter((child) => child.kind === 'dir').map((child) => child.path);
}

/** Was eine Taste im Explorer bedeutet. */
export type TreeIntent = 'up' | 'down' | 'open' | 'expand' | 'collapse' | 'toParent' | 'none';

/**
 * Die Taste, uebersetzt in eine Absicht, mit Blick auf die Zeile unter dem
 * Cursor.
 *
 * Rechts auf einem zugeklappten Ordner klappt auf, auf einem aufgeklappten geht
 * es in die erste Zeile darunter, also einfach nach unten. Links auf einem
 * aufgeklappten Ordner klappt zu, sonst springt es zum Elternordner. Das ist
 * das Verhalten, das jeder Baum in jeder IDE hat, und es steht hier als reine
 * Funktion, damit es ohne DOM pruefbar ist.
 */
export function treeIntent(key: string, row: TreeRow | undefined): TreeIntent {
    switch (key) {
        case 'ArrowDown':
            return 'down';
        case 'ArrowUp':
            return 'up';
        case 'ArrowRight':
            if (row === undefined || row.kind !== 'dir') {
                return 'none';
            }
            return row.expanded ? 'down' : 'expand';
        case 'ArrowLeft':
            if (row !== undefined && row.kind === 'dir' && row.expanded) {
                return 'collapse';
            }
            return 'toParent';
        case 'Enter':
        case ' ':
            if (row === undefined) {
                return 'none';
            }
            if (row.kind === 'file') {
                return 'open';
            }
            return row.expanded ? 'collapse' : 'expand';
        default:
            return 'none';
    }
}

/** Der Cursor nach einem Schritt, an den Enden festgehalten statt umgebrochen. */
export function moveCursor(rowCount: number, cursor: number, delta: number): number {
    if (rowCount <= 0) {
        return 0;
    }
    const next = cursor + delta;
    if (next < 0) {
        return 0;
    }
    if (next >= rowCount) {
        return rowCount - 1;
    }
    return next;
}

// ------------------------------------------------- Die Coverage-Quellen -----

/**
 * Ein Pfad, wie ihn beide Quellen schreiben sollten, und manchmal nicht tun.
 *
 * Der Graph liefert `src/services/userService.ts`, der Coverage-Store denselben
 * Pfad, und beide gelegentlich mit einem fuehrenden `./`. Ohne diese eine
 * Stelle waeren `./a.ts` und `a.ts` zwei Dateien, und der Join legte zwei
 * Zeilen fuer dieselbe Datei an.
 */
export function normalizeCoveragePath(path: string): string {
    let value = path.replace(/\\/g, '/').trim();
    while (value.startsWith('./')) {
        value = value.slice(2);
    }
    while (value.startsWith('/')) {
        value = value.slice(1);
    }
    while (value.endsWith('/')) {
        value = value.slice(0, -1);
    }
    return value;
}

/** Ein Eintrag der `parse_partial`-Liste: Pfad plus die gemeldeten Zeilenbereiche. */
export interface PartialFileEntry {
    path: string;
    /** Die Zeilenbereiche, in denen Konstrukte fehlen koennen. Roh, wie gemeldet. */
    errorRanges: string;
}

/** Ein Eintrag der `skipped`-Liste: Pfad, Grund und die Phase des Aufgebens. */
export interface SkippedFileEntry {
    path: string;
    reason: string;
    phase: string;
}

/** Ein Eintrag der `not_indexed.files`-Liste: Pfad und die Regel, die ihn ausschliesst. */
export interface NotIndexedFileEntry {
    path: string;
    reason: string;
}

/** Die Coverage-Beilage von `index_status`, so wie der Server sie schreibt. */
export interface IndexStatusCoverage {
    parsePartial: PartialFileEntry[];
    parsePartialCount: number;
    parsePartialTruncated: boolean;
    skipped: SkippedFileEntry[];
    skippedCount: number;
    skippedTruncated: boolean;
    notIndexedDirs: string[];
    notIndexedDirsCount: number;
    notIndexedFiles: NotIndexedFileEntry[];
    notIndexedFilesCount: number;
    notIndexedTruncated: boolean;
    /** Der Satz, den der Server selbst unter die Listen schreibt. Leer, wenn keiner. */
    note: string;
}

function coverageString(value: unknown): string {
    return typeof value === 'string' ? value : '';
}

function coverageArray(value: unknown): unknown[] {
    return Array.isArray(value) ? value : [];
}

/**
 * Die Coverage-Beilage aus einer rohen `index_status`-Antwort lesen.
 *
 * Nachsichtig gegen alles, was fehlt, und streng gegen das, was behauptet wird:
 * eine Antwort ohne die Beilage ergibt leere Listen mit Zahl null, nicht die
 * Behauptung, es gebe keine Luecken. Der Unterschied steht in der Oberflaeche:
 * dort ist "keine Liste" ein anderer Satz als "leere Liste".
 */
export function readIndexStatusCoverage(payload: unknown): IndexStatusCoverage {
    const raw = isRecord(payload) ? payload : {};
    const partial = isRecord(raw['parse_partial']) ? raw['parse_partial'] : {};
    const skipped = isRecord(raw['skipped']) ? raw['skipped'] : {};
    const notIndexed = isRecord(raw['not_indexed']) ? raw['not_indexed'] : {};

    const partialFiles: PartialFileEntry[] = [];
    for (const entry of coverageArray(partial['files'])) {
        if (!isRecord(entry)) {
            continue;
        }
        const path = normalizeCoveragePath(coverageString(entry['path']));
        if (path.length > 0) {
            partialFiles.push({ path, errorRanges: coverageString(entry['error_ranges']) });
        }
    }

    const skippedFiles: SkippedFileEntry[] = [];
    for (const entry of coverageArray(skipped['files'])) {
        if (!isRecord(entry)) {
            continue;
        }
        const path = normalizeCoveragePath(coverageString(entry['path']));
        if (path.length > 0) {
            skippedFiles.push({
                path,
                reason: coverageString(entry['reason']),
                phase: coverageString(entry['phase']),
            });
        }
    }

    const notIndexedDirs: string[] = [];
    for (const entry of coverageArray(notIndexed['dirs'])) {
        const path = normalizeCoveragePath(coverageString(entry));
        if (path.length > 0) {
            notIndexedDirs.push(path);
        }
    }

    const notIndexedFiles: NotIndexedFileEntry[] = [];
    for (const entry of coverageArray(notIndexed['files'])) {
        if (!isRecord(entry)) {
            continue;
        }
        const path = normalizeCoveragePath(coverageString(entry['path']));
        if (path.length > 0) {
            notIndexedFiles.push({ path, reason: coverageString(entry['reason']) });
        }
    }

    return {
        parsePartial: partialFiles,
        parsePartialCount: asNumber(partial['count']),
        parsePartialTruncated: partial['truncated'] === true,
        skipped: skippedFiles,
        skippedCount: asNumber(skipped['count']),
        skippedTruncated: skipped['truncated'] === true,
        notIndexedDirs,
        notIndexedDirsCount: asNumber(notIndexed['dirs_count']),
        notIndexedFiles,
        notIndexedFilesCount: asNumber(notIndexed['files_count']),
        notIndexedTruncated: notIndexed['truncated'] === true,
        note: coverageString(raw['coverage_note']),
    };
}

/** Eine Zeile des Coverage-Stores, wie `check_index_coverage` sie ausgibt. */
export interface CoverageRow {
    path: string;
    kind: string;
    detail: string;
    /** Nur bei einer Pfad-Abfrage: ob die Zeile den Pfad selbst oder einen Vorfahren meint. */
    match?: string;
}

/** Ein abgefragter Scope, mit seiner Seite und der Ansage, ob noch mehr kommt. */
export interface CoverageScope {
    requestedScope: string;
    scope: string;
    status: string;
    total: number;
    hasMore: boolean;
    nextOffset?: number;
    entries: CoverageRow[];
}

/** Die Antwort zu genau einem abgefragten Pfad. */
export interface CoveragePathAnswer {
    requestedPath: string;
    path: string;
    status: string;
    freshness: string;
    recommendedAction: string;
    coverage: CoverageRow[];
}

/**
 * Die Metadaten des Coverage-Stores.
 *
 * `ignoredFilesStored` und `ignoredFilesTotal` sind die einzige Stelle, an der
 * der Server ueberhaupt etwas ueber ignorierte Dateien sagt, ohne sie zu
 * nennen. Sie werden mitgelesen, weil "der Store kennt N ignorierte Dateien,
 * nennt aber keine" ein anderer Befund ist als "der Store kennt keine".
 */
export interface CoverageMetadata {
    generation: string;
    indexMode: string;
    recordedAt: string;
    recordingStatus: string;
    ignoredFilesStored: number;
    ignoredFilesTotal: number;
    hashRecordsComplete: boolean;
    coverageVersion: number;
    generationMatches: boolean;
}

/** Eine gelesene `check_index_coverage`-Antwort. */
export interface CoverageAnswer {
    project: string;
    metadata: CoverageMetadata;
    paths: CoveragePathAnswer[];
    scopes: CoverageScope[];
    caveat: string;
}

function readCoverageRows(value: unknown): CoverageRow[] {
    const rows: CoverageRow[] = [];
    for (const entry of coverageArray(value)) {
        if (!isRecord(entry)) {
            continue;
        }
        const path = normalizeCoveragePath(coverageString(entry['path']));
        if (path.length === 0) {
            continue;
        }
        const row: CoverageRow = {
            path,
            kind: coverageString(entry['kind']),
            detail: coverageString(entry['detail']),
        };
        const match = coverageString(entry['match']);
        if (match.length > 0) {
            row.match = match;
        }
        rows.push(row);
    }
    return rows;
}

/** Eine rohe `check_index_coverage`-Antwort lesen. */
export function readCoverageAnswer(payload: unknown): CoverageAnswer {
    const raw = isRecord(payload) ? payload : {};
    const meta = isRecord(raw['metadata']) ? raw['metadata'] : {};

    const paths: CoveragePathAnswer[] = [];
    for (const entry of coverageArray(raw['paths'])) {
        if (!isRecord(entry)) {
            continue;
        }
        paths.push({
            requestedPath: coverageString(entry['requested_path']),
            path: normalizeCoveragePath(coverageString(entry['path'])),
            status: coverageString(entry['status']),
            freshness: coverageString(entry['freshness']),
            recommendedAction: coverageString(entry['recommended_action']),
            coverage: readCoverageRows(entry['coverage']),
        });
    }

    const scopes: CoverageScope[] = [];
    for (const entry of coverageArray(raw['scopes'])) {
        if (!isRecord(entry)) {
            continue;
        }
        const scope: CoverageScope = {
            requestedScope: coverageString(entry['requested_scope']),
            scope: coverageString(entry['scope']),
            status: coverageString(entry['status']),
            total: asNumber(entry['total']),
            hasMore: entry['has_more'] === true,
            entries: readCoverageRows(entry['entries']),
        };
        if (entry['next_offset'] !== undefined) {
            scope.nextOffset = asNumber(entry['next_offset']);
        }
        scopes.push(scope);
    }

    return {
        project: coverageString(raw['project']),
        metadata: {
            generation: coverageString(meta['generation']),
            indexMode: coverageString(meta['index_mode']),
            recordedAt: coverageString(meta['recorded_at']),
            recordingStatus: coverageString(meta['recording_status']),
            ignoredFilesStored: asNumber(meta['ignored_files_stored']),
            ignoredFilesTotal: asNumber(meta['ignored_files_total']),
            hashRecordsComplete: meta['hash_records_complete'] === true,
            coverageVersion: asNumber(meta['coverage_version']),
            generationMatches: meta['generation_matches'] === true,
        },
        paths,
        scopes,
        caveat: coverageString(raw['caveat']),
    };
}

// --------------------------------------------------------- Der Join ---------

/** Ein Befund ueber genau einen Pfad, aus allen Quellen zusammengelegt. */
export interface CoverageRecord {
    path: string;
    kind: 'file' | 'dir';
    state: CoverageState;
    /** Der Grund des Servers. Leer heisst: er hat keinen genannt. */
    reason: string;
    /** Welche Quellen diesen Pfad genannt haben, in der Reihenfolge des Eintreffens. */
    sources: string[];
}

/** Was der Join ueber ein Projekt weiss. */
export interface CoverageIndex {
    /** Je Pfad genau ein Befund. Was hier fehlt, hat keine Quelle genannt. */
    records: ReadonlyMap<string, CoverageRecord>;
    /** Ehrliche Zeilen ueber Listen, die der Server gekappt hat. */
    truncations: string[];
    counts: {
        partial: number;
        skipped: number;
        notIndexedDirs: number;
        notIndexedFiles: number;
        scopeEntries: number;
    };
}

/** Ein Join ohne jede Quelle. Nicht "alles indiziert", sondern "noch nichts gefragt". */
export const EMPTY_COVERAGE: CoverageIndex = {
    records: new Map<string, CoverageRecord>(),
    truncations: [],
    counts: { partial: 0, skipped: 0, notIndexedDirs: 0, notIndexedFiles: 0, scopeEntries: 0 },
};

function putRecord(
    records: Map<string, CoverageRecord>,
    entry: { path: string; kind: 'file' | 'dir'; state: CoverageState; reason: string; source: string },
): void {
    const path = normalizeCoveragePath(entry.path);
    if (path.length === 0) {
        return;
    }
    const existing = records.get(path);
    if (existing === undefined) {
        records.set(path, {
            path,
            kind: entry.kind,
            state: entry.state,
            reason: entry.reason,
            sources: [entry.source],
        });
        return;
    }
    if (!existing.sources.includes(entry.source)) {
        existing.sources.push(entry.source);
    }
    // Ein Ordner bleibt ein Ordner: die Scope-Liste nennt Pfade ohne Art, und
    // eine Datei daraus duerfte einen bekannten Ordner nicht ueberschreiben.
    if (entry.kind === 'dir') {
        existing.kind = 'dir';
    }
    const worst = worstCoverage(existing.state, entry.state);
    if (worst !== existing.state) {
        existing.state = worst;
        existing.reason = entry.reason;
    } else if (existing.reason.length === 0) {
        existing.reason = entry.reason;
    }
}

/**
 * Die Quellen zu einem Befund je Pfad zusammenlegen.
 *
 * Der Graph steht hier nicht: er sagt ueber eine Datei nur, dass sie da ist,
 * und "da" ist die Abwesenheit eines Befundes. Wer in keiner Liste steht, gilt
 * als `indexed`, und genau das ist die Konfliktregel: eine Datei, die im
 * Graphen UND in `parse_partial` steht, ist partiell, nicht vollstaendig.
 */
export function buildCoverageIndex(input: {
    status?: IndexStatusCoverage | undefined;
    scopes?: readonly CoverageScope[] | undefined;
}): CoverageIndex {
    const records = new Map<string, CoverageRecord>();
    const truncations: string[] = [];
    const counts = { partial: 0, skipped: 0, notIndexedDirs: 0, notIndexedFiles: 0, scopeEntries: 0 };

    const status = input.status;
    if (status !== undefined) {
        for (const entry of status.parsePartial) {
            putRecord(records, {
                path: entry.path,
                kind: 'file',
                state: 'partial',
                reason: entry.errorRanges,
                source: 'index_status.parse_partial',
            });
        }
        for (const entry of status.skipped) {
            putRecord(records, {
                path: entry.path,
                kind: 'file',
                state: 'skipped',
                reason: [entry.reason, entry.phase].filter((part) => part.length > 0).join(' / '),
                source: 'index_status.skipped',
            });
        }
        for (const path of status.notIndexedDirs) {
            putRecord(records, {
                path,
                kind: 'dir',
                state: 'not-indexed',
                reason: '',
                source: 'index_status.not_indexed.dirs',
            });
        }
        for (const entry of status.notIndexedFiles) {
            putRecord(records, {
                path: entry.path,
                kind: 'file',
                state: 'not-indexed',
                reason: entry.reason,
                source: 'index_status.not_indexed.files',
            });
        }
        counts.partial = status.parsePartialCount;
        counts.skipped = status.skippedCount;
        counts.notIndexedDirs = status.notIndexedDirsCount;
        counts.notIndexedFiles = status.notIndexedFilesCount;

        if (status.parsePartialTruncated) {
            truncations.push(
                `the server cut the parse_partial list: ${status.parsePartialCount} recorded, fewer listed`,
            );
        }
        if (status.skippedTruncated) {
            truncations.push(
                `the server cut the skipped list: ${status.skippedCount} recorded, fewer listed`,
            );
        }
        if (status.notIndexedTruncated) {
            truncations.push(
                'the server cut the not_indexed list: '
                + `${status.notIndexedDirsCount} folders and ${status.notIndexedFilesCount} files recorded, fewer listed`,
            );
        }
    }

    for (const scope of input.scopes ?? []) {
        for (const row of scope.entries) {
            const state = coverageStateForKind(row.kind);
            putRecord(records, {
                path: row.path,
                kind: row.kind === 'not_indexed_dir' ? 'dir' : 'file',
                state,
                reason: row.detail,
                source: `check_index_coverage.scope:${scope.scope.length > 0 ? scope.scope : '.'}`,
            });
            counts.scopeEntries += 1;
        }
        if (scope.hasMore) {
            truncations.push(
                `the coverage store has more entries under "${scope.scope.length > 0 ? scope.scope : '.'}" `
                + `than were fetched: ${scope.total} recorded`,
            );
        }
    }

    return { records, truncations, counts };
}

/**
 * Die schlechteste Stufe unter einem Ordner, seine eigene eingeschlossen.
 *
 * Ueber den Coverage-Index und nicht ueber die geladenen Ebenen, und das ist
 * der Grund, warum der Punkt-Marker eines zugeklappten Ordners stimmt: die
 * Ebenen kommen einzeln und spaet, die Befunde kommen vollstaendig und frueh.
 */
export function aggregateCoverage(index: CoverageIndex, dirPath: string): CoverageState {
    const prefix = `${dirPath}/`;
    let worst: CoverageState = 'indexed';
    for (const record of index.records.values()) {
        if (record.path === dirPath || record.path.startsWith(prefix)) {
            worst = worstCoverage(worst, record.state);
        }
    }
    return worst;
}

function ensureLevel(levels: Map<string, TreeLevel>, path: string): TreeLevel {
    const existing = levels.get(path);
    if (existing !== undefined) {
        return existing;
    }
    const created: TreeLevel = {
        path,
        files: 0,
        symbols: 0,
        children: [],
        foldedDuplicates: 0,
        droppedNonPaths: 0,
        synthetic: true,
    };
    levels.set(path, created);
    return created;
}

function graftPath(levels: Map<string, TreeLevel>, path: string, kind: 'file' | 'dir'): void {
    const parent = parentPath(path);
    if (parent.length > 0) {
        graftPath(levels, parent, 'dir');
    }
    const level = ensureLevel(levels, parent);
    if (level.children.some((child) => child.path === path)) {
        return;
    }
    level.children.push({
        name: path.slice(path.lastIndexOf('/') + 1),
        path,
        kind,
        symbols: 0,
        files: 0,
    });
}

/**
 * Die Vereinigung: die Ebenen aus dem Graphen, ergaenzt um jeden Pfad, den nur
 * die Coverage-Listen kennen, und jede Zeile mit ihrer Stufe versehen.
 *
 * Die Ebenen werden kopiert und nicht veraendert. Ein Join, der die Antworten
 * des Servers an Ort und Stelle beschriftet, macht aus zweimal Lesen zweimal
 * etwas anderes, und der Explorer zeichnet oefter, als der Server antwortet.
 */
export function mergeCoverageIntoLevels(
    levels: ReadonlyMap<string, TreeLevel>,
    index: CoverageIndex,
): Map<string, TreeLevel> {
    const merged = new Map<string, TreeLevel>();
    for (const [path, level] of levels) {
        merged.set(path, { ...level, children: level.children.map((child) => ({ ...child })) });
    }

    for (const record of index.records.values()) {
        graftPath(merged, record.path, record.kind);
    }

    for (const level of merged.values()) {
        for (const child of level.children) {
            const record = index.records.get(child.path);
            child.coverage = child.kind === 'dir'
                ? aggregateCoverage(index, child.path)
                : record?.state ?? 'indexed';
            if (record !== undefined && record.reason.length > 0) {
                child.coverageReason = record.reason;
            }
            if (record !== undefined) {
                child.coverageSources = [...record.sources];
            }
        }
        level.children.sort(compareChildren);
    }

    return merged;
}
