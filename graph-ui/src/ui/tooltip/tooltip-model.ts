/**
 * Wo ein Tooltip stehen darf, und was er dabei nicht verdecken darf.
 *
 * ## Der Befund, gegen den diese Datei geschrieben ist
 *
 * Nutzerbefund vom 2026-08-29 mit Screenshot: der Tooltip "76 nodes, 178 edges
 * from /api/layout" lag ueber dem Detail-Regler und ueber den Chips Logic,
 * Calls und Data. Der Nutzer dazu: "ich hab damals schon in Auftrag gegeben,
 * dass sich Dinge ueberlappen, was uncool ist."
 *
 * Die Ursache war nicht ein falsch gesetzter Kasten, sondern eine ganze
 * Gattung: es waren native `title`-Tooltips, 78 davon im Produktivcode. Der
 * Browser zeichnet sie AUSSERHALB des Dokuments, unter dem Mauszeiger. Sie
 * haben kein Rechteck, das `getBoundingClientRect` liefern koennte, und darum
 * hat keine einzige Ueberlagerungsmessung dieses Projekts sie je gesehen. Es
 * gab also 78 Flaechen, die sich ueber beliebigen Inhalt legen konnten, und
 * null Messung darauf.
 *
 * Ein eigener Tooltip im Dokument loest beide Haelften: er hat ein Rechteck,
 * also kann ein Beweislauf ihn sehen, und er kann RECHNEN, wo er hin soll,
 * statt unter dem Zeiger zu erscheinen. Diese Datei ist die Rechnung. Sie steht
 * ohne React und ohne DOM da, damit jede Zahl ohne Browser pruefbar ist.
 *
 * ## Die Regel
 *
 * Ein Tooltip darf nicht verdecken, was der Leser gerade BRAUCHT. Das ist enger
 * als "nichts verdecken": ein Tooltip liegt per Bauart ueber etwas, sonst
 * muesste die Oberflaeche ihm Platz freihalten, den sie nicht hat. Geschuetzt
 * ist darum genau dreierlei:
 *
 *  1. sein eigener Ausloeser (ein Tooltip, der den Knopf verdeckt, den man
 *     gerade beruehrt, nimmt dem Zeiger sein Ziel),
 *  2. Regler und Eingabefelder (das ist der Fall aus dem Screenshot),
 *  3. die Beschriftungen der Sektion, in der der Ausloeser steht (`data-hint-keep`).
 *
 * Alles andere darf ein Tooltip ueberdecken: Fliesstext, den man nach dem
 * Loslassen wieder sieht, ist kein Verlust.
 *
 * ## Warum zwoelf Kandidaten und nicht vier
 *
 * Vier Seiten mal drei Ausrichtungen auf der Querachse. Mit nur vier Seiten
 * scheitert die Regel an genau den Stellen, an denen sie gebraucht wird: ein
 * Ausloeser am oberen Rand einer Spalte hat unter sich seine eigene Werkzeug-
 * leiste und ueber sich das Fensterende, und "unten, links ausgerichtet" ist
 * dann besetzt, waehrend "unten, rechts ausgerichtet" frei danebenliegt. Die
 * Ausrichtung ist billig zu rechnen und macht den Unterschied zwischen einer
 * Regel, die haelt, und einer, die eine Ausnahmeliste braucht.
 */

/** Ein Rechteck in Fensterkoordinaten. Dieselbe Form wie `DOMRect`, ohne dessen Rest. */
export interface HintRect {
    x: number;
    y: number;
    width: number;
    height: number;
}

/** Auf welcher Seite des Ausloesers der Kasten steht. */
export type HintSide = 'below' | 'above' | 'right' | 'left';

/** Wie er auf der Querachse zum Ausloeser liegt. */
export type HintAlign = 'start' | 'center' | 'end';

/** Das Ergebnis der Rechnung: wo der Kasten hingehoert und was er dort verdeckt. */
export interface HintPlacement {
    side: HintSide;
    align: HintAlign;
    x: number;
    y: number;
    /** Die verdeckte Flaeche in Quadratpixeln. Null ist die bestandene Antwort. */
    covered: number;
    /** Ob der Kasten auf dieser Seite ueberhaupt ganz hineinpasst. */
    fits: boolean;
}

/** Der Abstand zwischen Ausloeser und Kasten. Gross genug, dass man die Kante sieht. */
export const HINT_GAP = 6;

/** Der Rand zum Fenster. Ein Kasten, der die Kante beruehrt, sieht abgeschnitten aus. */
export const HINT_MARGIN = 8;

/**
 * Was ein Tooltip nicht verdecken darf, als Daten und mit Grund.
 *
 * Als Liste und nicht als Bedingung im Code, weil sie an zwei Stellen gilt: die
 * Oberflaeche haelt sich beim Platzieren daran (diese Datei), und der
 * Beweislauf misst danach dasselbe (tools/lib/readability.mjs). Zwei
 * Formulierungen derselben Regel waeren zwei Regeln.
 */
export const HINT_PROTECTED: readonly { selector: string; reason: string }[] = [
    {
        selector: 'input, textarea, select',
        reason:
            'Regler und Eingabefelder. Genau der Fall aus dem Screenshot vom 2026-08-29: '
            + 'ein Tooltip lag ueber dem Detail-Regler des Twin.',
    },
    {
        selector: '[data-hint-keep]',
        reason:
            'Die Beschriftungen und Schalter der Sektion, in der der Ausloeser steht. Wer einen '
            + 'Knopf beruehrt, um zu erfahren was er tut, schaut danach auf seine Nachbarn.',
    },
];

/** Der Selektor der geschuetzten Flaechen, in einem Stueck. */
export const HINT_PROTECTED_SELECTOR = HINT_PROTECTED.map((entry) => entry.selector).join(', ');

/** Wie breit ein Kasten hoechstens wird. Dieselbe Zahl steht als `max-width` in der CSS-Datei. */
export const HINT_MAX_WIDTH = 320;

/** Die Flaeche, die zwei Rechtecke gemeinsam haben. Null, wenn sie sich nicht beruehren. */
export function overlapArea(a: HintRect, b: HintRect): number {
    const width = Math.min(a.x + a.width, b.x + b.width) - Math.max(a.x, b.x);
    const height = Math.min(a.y + a.height, b.y + b.height) - Math.max(a.y, b.y);
    return width > 0 && height > 0 ? width * height : 0;
}

/** Einen Wert in einen Bereich zwingen. */
function clamp(value: number, low: number, high: number): number {
    return Math.max(low, Math.min(high, value));
}

/** Die zwoelf Kandidaten, in der Reihenfolge, in der sie geprueft werden. */
const CANDIDATES: readonly { side: HintSide; align: HintAlign }[] = [
    { side: 'below', align: 'start' },
    { side: 'below', align: 'end' },
    { side: 'below', align: 'center' },
    { side: 'above', align: 'start' },
    { side: 'above', align: 'end' },
    { side: 'above', align: 'center' },
    { side: 'right', align: 'start' },
    { side: 'right', align: 'end' },
    { side: 'right', align: 'center' },
    { side: 'left', align: 'start' },
    { side: 'left', align: 'end' },
    { side: 'left', align: 'center' },
];

export interface HintPlacementInput {
    anchor: HintRect;
    size: { width: number; height: number };
    viewport: { width: number; height: number };
    /** Was nicht verdeckt werden darf. Der Ausloeser gehoert nicht hinein, er kommt von selbst dazu. */
    protect: readonly HintRect[];
    gap?: number;
    margin?: number;
}

/**
 * Das Rechteck, aus dem der Kasten heraus muss.
 *
 * Der Ausloeser plus jede geschuetzte Flaeche, die ihn ENTHAELT. Das ist der
 * Unterschied zwischen einer Regel, die haelt, und einer, die nur so aussieht:
 * ein Chip in der Werkzeugleiste des Twin hat unter sich die naechste Zeile
 * DERSELBEN Leiste, und ein Kasten "unter dem Chip" liegt damit mitten in dem,
 * was der Contract schuetzt. Ausgewichen wird darum nicht dem Knopf, sondern
 * der Sektion, in der er steht: das ist genau die Formulierung aus AC2
 * ("Beschriftungen der Sektion, in der er steht").
 *
 * Enthalten heisst hier geometrisch und nicht ueber den DOM-Baum, damit die
 * Rechnung ohne Dokument prueffaehig bleibt. Der Unterschied ist an dieser
 * Oberflaeche keiner: eine Flaeche, die den Ausloeser umschliesst, ist seine
 * Sektion, ob sie ihn nun auch im Baum enthaelt oder nicht.
 */
export function avoidFrameOf(anchor: HintRect, protect: readonly HintRect[]): HintRect {
    let left = anchor.x;
    let top = anchor.y;
    let right = anchor.x + anchor.width;
    let bottom = anchor.y + anchor.height;
    for (const rect of protect) {
        const holds = rect.x <= anchor.x + 1
            && rect.y <= anchor.y + 1
            && rect.x + rect.width >= anchor.x + anchor.width - 1
            && rect.y + rect.height >= anchor.y + anchor.height - 1;
        if (!holds) {
            continue;
        }
        left = Math.min(left, rect.x);
        top = Math.min(top, rect.y);
        right = Math.max(right, rect.x + rect.width);
        bottom = Math.max(bottom, rect.y + rect.height);
    }
    return { x: left, y: top, width: right - left, height: bottom - top };
}

/**
 * Den Kasten aus dem Weg schieben, wenn keine der zwoelf Lagen frei ist.
 *
 * ## Der Befund, gegen den diese Funktion geschrieben ist
 *
 * Gemessen am 2026-08-30 (`npm run smoke:w8b`): der Tooltip einer Schrittzeile
 * lag ueber der Kommandozeile. Die Ursache steckt in der Begrenzung selbst: die
 * zwoelf Kandidaten werden ins FENSTER gezwungen (`clamp`), und der untere Rand
 * des Fensters gehoert der Kommandozeile. Ein Ausloeser, der so weit unten
 * steht, dass jede Lage an diesen Rand geklemmt wird, bekommt damit
 * zwangslaeufig einen Kasten ueber einer geschuetzten Flaeche, obwohl zehn
 * Pixel darueber frei sind.
 *
 * ## Was sie tut, und was ausdruecklich nicht
 *
 * Sie setzt den Kasten an die Kante des Hindernisses, das er verdeckt: knapp
 * darueber, darunter, links oder rechts davon. Genommen wird die kleinste
 * Bewegung, die ganz frei ist und im Fenster bleibt; gibt es keine, bleibt es
 * bei der Lage von vorher, und `covered` sagt weiterhin die Wahrheit ueber sie.
 *
 * EIN Schritt und keine Suche ueber mehrere Hindernisse: eine Rechnung, die
 * sich durch eine Landschaft aus Rechtecken tastet, ist an einer Oberflaeche,
 * die zwoelf Lagen anbietet, nicht mehr nachvollziehbar. Sie laeuft ausserdem
 * nur, wenn KEINE der zwoelf Lagen frei war; die ueblichen Faelle sehen sie
 * nie.
 */
export function slideClear(
    box: HintRect,
    viewport: { width: number; height: number },
    protect: readonly HintRect[],
    margin: number,
): HintRect | null {
    const covers = (candidate: HintRect): number =>
        protect.reduce((sum, rect) => sum + overlapArea(candidate, rect), 0);
    const inside = (candidate: HintRect): boolean =>
        candidate.x >= margin
        && candidate.y >= margin
        && candidate.x + candidate.width <= viewport.width - margin
        && candidate.y + candidate.height <= viewport.height - margin;

    const tries: HintRect[] = [];
    for (const rect of protect) {
        if (overlapArea(box, rect) === 0) {
            continue;
        }
        tries.push({ ...box, y: rect.y - box.height - 1 });
        tries.push({ ...box, y: rect.y + rect.height + 1 });
        tries.push({ ...box, x: rect.x - box.width - 1 });
        tries.push({ ...box, x: rect.x + rect.width + 1 });
    }
    const moved = (candidate: HintRect): number =>
        Math.abs(candidate.x - box.x) + Math.abs(candidate.y - box.y);
    const free = tries.filter((candidate) => inside(candidate) && covers(candidate) === 0);
    if (free.length === 0) {
        return null;
    }
    return free.reduce((best, candidate) => (moved(candidate) < moved(best) ? candidate : best));
}

/**
 * Wo der Kasten hingehoert.
 *
 * Zuerst der erste Kandidat, der ganz ins Fenster passt UND nichts Geschuetztes
 * verdeckt. Gibt es keinen, der beste unter denen, die passen, und der wird
 * danach noch aus dem Weg geschoben, wenn daneben Platz ist ({@link
 * slideClear}). Gibt es auch davon keinen (ein Fenster, das kleiner ist als der
 * Kasten), der mit der kleinsten verdeckten Flaeche ueberhaupt. Total: es kommt
 * immer eine Lage heraus, und sie sagt in `covered` und `fits` selbst, wie gut
 * sie ist, statt dass der Aufrufer raten muss.
 */
export function placeHint(input: HintPlacementInput): HintPlacement {
    const gap = input.gap ?? HINT_GAP;
    const margin = input.margin ?? HINT_MARGIN;
    const { anchor, size, viewport } = input;
    const protect = [anchor, ...input.protect];
    /*
     * Ausgewichen wird der SEKTION und nicht dem Knopf. Warum, steht an
     * {@link avoidFrameOf}; auf der Querachse richtet sich der Kasten weiter am
     * Ausloeser aus, damit sichtbar bleibt, wozu er gehoert.
     */
    const frame = avoidFrameOf(anchor, input.protect);

    const results: HintPlacement[] = CANDIDATES.map(({ side, align }) => {
        let x = 0;
        let y = 0;
        let fits = true;

        if (side === 'below' || side === 'above') {
            y = side === 'below' ? frame.y + frame.height + gap : frame.y - gap - size.height;
            fits = side === 'below'
                ? y + size.height <= viewport.height - margin
                : y >= margin;
            x = align === 'start'
                ? anchor.x
                : align === 'end'
                    ? anchor.x + anchor.width - size.width
                    : anchor.x + anchor.width / 2 - size.width / 2;
            x = clamp(x, margin, Math.max(margin, viewport.width - margin - size.width));
            y = clamp(y, margin, Math.max(margin, viewport.height - margin - size.height));
        } else {
            x = side === 'right' ? frame.x + frame.width + gap : frame.x - gap - size.width;
            fits = side === 'right'
                ? x + size.width <= viewport.width - margin
                : x >= margin;
            y = align === 'start'
                ? anchor.y
                : align === 'end'
                    ? anchor.y + anchor.height - size.height
                    : anchor.y + anchor.height / 2 - size.height / 2;
            x = clamp(x, margin, Math.max(margin, viewport.width - margin - size.width));
            y = clamp(y, margin, Math.max(margin, viewport.height - margin - size.height));
        }

        const box: HintRect = { x, y, width: size.width, height: size.height };
        const covered = protect.reduce((sum, rect) => sum + overlapArea(box, rect), 0);
        return { side, align, x, y, covered, fits };
    });

    const clean = results.find((entry) => entry.fits && entry.covered === 0);
    if (clean !== undefined) {
        return clean;
    }
    const fitting = results.filter((entry) => entry.fits);
    const pool = fitting.length > 0 ? fitting : results;
    const best = pool.reduce((entry, next) => (next.covered < entry.covered ? next : entry), pool[0]);
    const free = slideClear(
        { x: best.x, y: best.y, width: size.width, height: size.height },
        viewport,
        protect,
        margin,
    );
    if (free === null) {
        return best;
    }
    return {
        ...best,
        x: free.x,
        y: free.y,
        covered: protect.reduce((sum, rect) => sum + overlapArea(free, rect), 0),
    };
}

/**
 * Ob ein Satz mehr sagt als das, was ohnehin dasteht.
 *
 * Die eine Frage, an der AC1 haengt: ein `title`, der den sichtbaren Text
 * wiederholt, faellt ersatzlos weg, und einer, der etwas ERKLAERT, wird zu
 * einem eigenen Tooltip. Verglichen wird nach Leerraum-Normalisierung und ohne
 * Ansehen der Gross-/Kleinschreibung, weil "GALAXY" und "galaxy" fuer einen
 * Leser dasselbe Wort sind.
 *
 * Hier als Funktion und nicht nur als Regel im Kopf, weil der Beweislauf
 * dieselbe Frage im Browser stellt: was dort `nativeTitlesWithExplanation`
 * zaehlt, ist genau diese Bedingung.
 */
export function explainsMore(title: string, visibleText: string): boolean {
    const tidy = (value: string): string => value.replace(/\s+/g, ' ').trim().toLowerCase();
    const said = tidy(title);
    if (said.length === 0) {
        return false;
    }
    return !tidy(visibleText).includes(said);
}
