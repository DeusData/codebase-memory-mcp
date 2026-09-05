/**
 * Vom Dateipfad zum qualifizierten Namen, den der Graph fuer diese Datei fuehrt.
 *
 * Der Reader braucht diese Umrechnung, weil der Server keinen Endpunkt hat, der
 * eine Datei ausliefert (siehe INVENTAR.md Abschnitt 3 und Ask 1). Was es gibt,
 * ist `get_code_snippet` auf einem indizierten Symbol, und der Modul-Knoten ist
 * das Symbol, das die ganze Datei umspannt.
 *
 * Die Regel ist an einem laufenden Server abgelesen, nicht geraten
 * (fixtures/atlas-sample, indiziert, `MATCH (n:Module) RETURN n.qualified_name,
 * n.file_path`):
 *
 *   src/services/userService.ts   -> <projekt>.src.services.userService
 *   test/userService.test.ts      -> <projekt>.test.userService.test
 *   HERKUNFT.md                   -> <projekt>.HERKUNFT
 *
 * Also: Schraegstriche werden zu Punkten, und **genau eine** Endung faellt weg,
 * die letzte. `userService.test.ts` behaelt sein `.test`, weil der Graph es
 * behaelt.
 *
 * Der File-Knoten derselben Datei traegt den ganzen Pfad samt Endung und haengt
 * `.__file__` an:
 *
 *   src/services/userService.ts   -> <projekt>.src.services.userService.ts.__file__
 *
 * Damit ist die Ableitung gegenpruefbar, und der Reader prueft sie auch: er
 * fragt den Graphen nach dem Modul-Knoten dieser Datei und nimmt dessen Namen,
 * wenn er von der Ableitung abweicht. Die Ableitung ist der schnelle Weg, der
 * Graph bleibt die Wahrheit.
 */

/** Was `.__file__` an einen File-Knoten haengt. */
export const FILE_NODE_SUFFIX = '.__file__';

/** Ein Pfad ohne fuehrenden Schraegstrich und ohne `./`. */
export function normalizeWorkspacePath(path: string): string {
    let value = path.trim().replace(/\\/g, '/');
    while (value.startsWith('./')) {
        value = value.slice(2);
    }
    while (value.startsWith('/')) {
        value = value.slice(1);
    }
    return value;
}

/**
 * Der Pfad ohne seine letzte Endung.
 *
 * Ein Punkt im Verzeichnisteil zaehlt nicht (`.github/workflows/ci.yml` verliert
 * `.yml`, nicht `.github/...`), und ein Name, der nur aus einer Endung besteht
 * (`.gitignore`), behaelt sie: sonst bliebe ein leerer Name uebrig.
 */
export function stripLastExtension(path: string): string {
    const slash = path.lastIndexOf('/');
    const base = slash === -1 ? path : path.slice(slash + 1);
    const dot = base.lastIndexOf('.');
    if (dot <= 0) {
        return path;
    }
    return (slash === -1 ? '' : path.slice(0, slash + 1)) + base.slice(0, dot);
}

/** Der qualifizierte Name des Modul-Knotens, den der Graph fuer diese Datei fuehrt. */
export function moduleQualifiedName(project: string, filePath: string): string {
    const path = normalizeWorkspacePath(filePath);
    const withoutExtension = stripLastExtension(path);
    return `${project}.${withoutExtension.split('/').join('.')}`;
}

/** Der qualifizierte Name des File-Knotens derselben Datei. */
export function fileQualifiedName(project: string, filePath: string): string {
    const path = normalizeWorkspacePath(filePath);
    return `${project}.${path.split('/').join('.')}${FILE_NODE_SUFFIX}`;
}

/**
 * Der Modul-Name, der zu einem File-Knoten gehoert: Suffix ab, Endung ab.
 *
 * Das ist der Rueckweg und er existiert, damit die Ableitung gegen eine Antwort
 * des Servers gehalten werden kann, statt gegen eine zweite Ableitung.
 */
export function moduleQnFromFileQn(fileQualifiedNameValue: string): string | undefined {
    if (!fileQualifiedNameValue.endsWith(FILE_NODE_SUFFIX)) {
        return undefined;
    }
    const withExtension = fileQualifiedNameValue.slice(0, -FILE_NODE_SUFFIX.length);
    const dot = withExtension.lastIndexOf('.');
    return dot <= 0 ? withExtension : withExtension.slice(0, dot);
}

/** Die Segmente eines Pfades, wie die Breadcrumb sie zeigt. */
export function pathSegments(filePath: string): string[] {
    return normalizeWorkspacePath(filePath).split('/').filter((segment) => segment.length > 0);
}

/** Der Dateiname ohne Verzeichnisse. Der Name eines Tabs. */
export function baseName(filePath: string): string {
    const segments = pathSegments(filePath);
    return segments.length === 0 ? filePath : segments[segments.length - 1];
}
