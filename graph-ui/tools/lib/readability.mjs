/**
 * Das Lesbarkeits-Gate: liegt an dieser Stelle der Strecke etwas uebereinander,
 * ragt etwas aus seinem Kasten, und was passiert, wenn man bis ans Ende
 * scrollt?
 *
 * Bernhards Schlussanforderung vom 2026-08-29, und sie ist keine Kosmetikfrage.
 * Vier Wellen Rueckmeldung in W5c handelten davon, dass Beschriftungen sich
 * ueberlagerten, dass ein Kasten hinter einer Zeile verschwand und dass eine
 * Leiste in ihre Nachbarn wuchs. Jedes Mal war der Befund an einem gruenen
 * Testlauf nicht zu sehen und an einem Screenshot sofort. Dieses Modul macht die
 * Frage messbar, damit sie an JEDEM Halt gestellt wird und nicht nur dort, wo
 * gerade jemand hinsieht.
 *
 * ## Was gemessen wird
 *
 * **Kandidaten** sind sichtbare Elemente mit eigenem Text: mindestens ein
 * direktes Textkind mit Zeichen darin, ein Rechteck mit Flaeche, nicht
 * `display:none`, nicht `visibility:hidden`, nicht durchsichtig. Ein Element,
 * das nur andere Elemente enthaelt, ist kein Kandidat: sein Rechteck ist die
 * Summe seiner Kinder und wuerde jedes von ihnen ueberlagern.
 *
 * **Sichtbar heisst wirklich sichtbar.** Gerechnet wird nicht mit dem Rechteck
 * des Elements, sondern mit dem, was davon nach allen Kaesten darueber uebrig
 * bleibt: das Rechteck wird an jedem Vorfahren geschnitten, dessen `overflow`
 * nicht `visible` ist, und zuletzt am Fenster. Bleibt nichts uebrig, ist das
 * Element nicht auf dem Bildschirm und zaehlt nicht mit. Ohne diesen Schnitt
 * waere jeder gescrollte Bereich eine Fundgrube falscher Befunde: eine Zeile,
 * die im Twin nach oben aus dem Sichtfenster gewandert ist, hat weiterhin ein
 * Rechteck, und das liegt dann rechnerisch ueber der Ueberschrift des Panels
 * darunter, obwohl der Leser sie gar nicht sieht.
 *
 * **(a) Ueberlagerung.** Zwei Kandidaten derselben EBENE, von denen keiner den
 * anderen enthaelt, duerfen sich mit ihren SICHTBAREN Rechtecken nicht
 * schneiden. Die Ebene eines Elements ist sein naechster Vorfahre (es selbst
 * eingeschlossen), der absolut, fest oder klebend positioniert ist; hat er
 * keinen, ist die Ebene das Dokument. Das ist die Regel und nicht eine Liste
 * von Ausnahmen, und sie trifft genau den Fehler, um den es geht: ein Overlay
 * DARF ueber dem Text liegen, den es verdeckt, das ist sein Zweck. Zwei
 * Beschriftungen IM Overlay duerfen es nicht. Welche Ebenen ein Halt hatte,
 * steht im Ergebnis, damit die Ausnahme nachlesbar ist statt behauptet.
 *
 * **(b) Beschneidung.** Ein sichtbarer Kandidat darf nicht aus einem Kasten
 * ragen, der ihn abschneidet und den man nicht aufscrollen kann. Gesucht wird
 * der naechste Vorfahre mit `overflow: hidden` oder `clip` auf der jeweiligen
 * Achse; ein `auto` oder `scroll` zaehlt nicht, denn was dort heraushaengt, ist
 * erreichbar, und Erreichbarkeit ist genau der Unterschied zwischen "steht
 * weiter unten" und "ist weg".
 *
 * Zwei Schwellen gehoeren dazu, und sie sind verschieden, weil die beiden
 * Achsen verschieden sind. SENKRECHT ist der Kasten einer Textzeile fast immer
 * ein paar Pixel hoeher als die Schrift darin (halbe Zeilendurchschuss ueber
 * und unter den Buchstaben); ein Ueberstand von vier Pixeln schneidet dort
 * nichts ab, was ein Leser sehen wuerde. Gemeldet wird deshalb erst, was mehr
 * als ein gutes Drittel der Elementhoehe verdeckt. WAAGERECHT gibt es diesen
 * Puffer nicht: was rechts abgeschnitten wird, sind Buchstaben. Dort zaehlt
 * schon ein Ueberstand von zwei Pixeln, es sei denn, der Kasten sagt mit
 * `text-overflow: ellipsis` selbst, dass er kuerzt: drei Punkte sind eine
 * Ansage an den Leser und kein verschwundener Text.
 *
 * **(c) Eine Zeile, die an einer Bildlaufkante halb dasteht.** Ein Kasten, der
 * scrollt, schneidet nichts weg: was heraushaengt, ist erreichbar, und deshalb
 * meldet (b) ihn nicht. Das stimmt fuer den Explorer, in dem viele Dateien
 * scrollen duerfen, und es war fuer die Legende der Galaxie zu nachsichtig
 * (Bernhards Befund an den Beweisbildern von W9, 2026-08-29): dort stand ueber
 * der ersten Zeile ein Rest wie "not more importance:" und unten endete ein
 * Satz mitten im Wort. Die Zahlen waren gruen, die Bilder zeigten einen
 * Darstellungsfehler.
 *
 * Gefragt wird darum am RUHEZUSTAND und an der ZEILE, und beide Einschraenkungen
 * sind der Grund, dass die Regel eng bleibt:
 *
 *  - **Ruhezustand.** Nur ein Kasten, der noch am Anfang steht
 *    (`scrollTop`/`scrollLeft` bei null), wird gefragt. Die Regel stellt die
 *    Frage, die ein Leser beim OEFFNEN stellt: "ist das alles?". Wer selbst
 *    gescrollt hat, hat die Antwort schon; und ein Kasten, den die Messung
 *    unter (c) selbst ans Ende gefahren hat, zeigt einen Bildschirm, den dieses
 *    Produkt nie ausliefert.
 *  - **Zeile.** Gemeldet wird nicht, dass Inhalt ausserhalb steht (das ist der
 *    Sinn eines Bildlaufs), sondern dass die Kante MITTEN DURCH eine Textzeile
 *    geht: ein Teil der Zeile ist zu sehen, der andere nicht. Gemessen an den
 *    Zeilenrechtecken des Textes selbst (`Range.getClientRects`) und nicht am
 *    Rechteck des Absatzes, denn ein fuenfzeiliger Absatz, dem die halbe letzte
 *    Zeile fehlt, ist an seinem Gesamtrechteck fast vollstaendig.
 *
 * Steht die Kante mitten in einer Zeile, dann muss der Kasten das SAGEN. Als
 * Hinweis zaehlt, was ein Leser im Bild sieht: eine Marke mit
 * `data-scroll-hint` fuer diese Kante, ein Verlauf ueber der Kante (eine
 * Maske), ein Streifen mit einem Farbverlauf, der an dieser Kante liegt (so
 * macht es die Tab-Leiste), ein mitrollender Hintergrund (das Schatten-Idiom
 * mit `background-attachment: local`) oder eine Bildlaufleiste, die wirklich
 * Platz einnimmt. Fehlt alles davon, ist es eine Verletzung: der Leser sieht einen
 * abgeschnittenen Satz und nichts, was ihm sagt, dass es weitergeht. Die
 * Verletzung steht in derselben Liste wie (b) und traegt `kind`, damit jeder
 * Lauf sie ohne eigene Aenderung mitzaehlt und ein Leser des Artefakts die
 * beiden Faelle trotzdem auseinanderhaelt.
 *
 * **(d) Scrollen.** Jeder sichtbare Bereich, der wirklich mehr Inhalt hat als
 * Platz, wird ans Ende gefahren und dort erneut nach (a) und (b) gefragt. Ein
 * Panel, das oben ordentlich aussieht und unten kollidiert, ist ein Panel, das
 * kollidiert.
 *
 * ## Die eine Ausnahme, und warum sie eine ist
 *
 * Die Innereien des Editors werden nicht gemessen. Monaco setzt seine Zeilen,
 * seine Zeilennummern und seine Dekorationen selbst, absolut positioniert und
 * pixelgenau, und es tut das in einer eigenen Welt aus ueberlagerten Ebenen.
 * Sie zu messen hiesse, eine fremde Bibliothek gegen die Layoutregeln dieses
 * Produkts zu pruefen. Der Editor als FLAECHE wird gemessen, seine Nachbarn
 * auch; was er in sich zeichnet, verantwortet er. Die Ausnahme steht als
 * `exclusions` im Ergebnis.
 */

/**
 * **(e) Die Tooltips, und warum sie eine eigene Frage brauchen.**
 *
 * Bis W8b erklaerte sich diese Oberflaeche mit dem `title`-Attribut, 78 Mal.
 * Der Browser zeichnet daraus einen Kasten AUSSERHALB des Dokuments, unter dem
 * Mauszeiger und ohne Rechteck. Er hat damit keine der Eigenschaften, an denen
 * (a) bis (d) messen: kein `getBoundingClientRect`, keine Ebene, kein
 * Vorfahre. Es gab also 78 Flaechen, die sich ueber beliebigen Inhalt legen
 * konnten, und null Messung darauf. Der Nutzer hat am 2026-08-29 fotografiert,
 * was daraus folgt: der Tooltip der Kopfzeile lag ueber dem Detail-Regler und
 * ueber den Chips Logic, Calls und Data.
 *
 * Seit W8b zeichnet diese Oberflaeche ihre Tooltips selbst (src/ui/tooltip/).
 * Sie liegen im Dokument, also sieht (a) sie grundsaetzlich; nur greift die
 * Ebenen-Regel bei ihnen absichtlich nicht, denn ein Tooltip ist fest
 * positioniert und damit seine EIGENE Ebene. Das ist richtig so: ein Tooltip
 * DARF ueber Fliesstext liegen, das ist seine Bauart. Was er nicht darf, ist
 * das hier, und es ist eine engere Frage als die von (a):
 *
 *   Ein offener Tooltip darf nichts verdecken, was der Leser gerade BRAUCHT.
 *
 * Gebraucht heisst geschuetzt, und geschuetzt ist genau dreierlei: der
 * Ausloeser selbst (ein Kasten, der den Knopf verdeckt, den man beruehrt, nimmt
 * dem Zeiger sein Ziel), Regler und Eingabefelder, und die Flaechen, die die
 * Oberflaeche mit `data-hint-keep` als Beschriftung ihrer Sektion ausweist.
 * Dieselbe Liste, nach der die Oberflaeche selbst platziert (siehe
 * src/ui/tooltip/tooltip-model.ts): zwei Formulierungen derselben Regel waeren
 * zwei Regeln, und der Beweislauf soll die Zusicherung pruefen und nicht eine
 * zweite daneben aufstellen.
 *
 * Der Lauf oeffnet dafuer JEDEN Tooltip einzeln und misst; die Zahl der so
 * gemessenen steht als `domTooltipsMeasured` im Artefakt. Ein Lauf, der einmal
 * hinsieht und "keine Ueberlagerung" meldet, haette ueber die 77 anderen
 * nichts gesagt.
 */

/**
 * Der Selektor, dessen Inhalt nicht gemessen wird, mit Begruendung.
 *
 * Als Datenstruktur und nicht als Kommentar, weil sie in das Beweisartefakt
 * gehoert: eine Ausnahme, die nur im Quelltext steht, kann ein Leser des
 * Ergebnisses nicht sehen.
 */
export const READABILITY_EXCLUSIONS = [
    {
        selector: '.monaco-editor',
        reason:
            'Der Editor setzt Zeilen, Zeilennummern und Dekorationen selbst, absolut positioniert '
            + 'und in eigenen Ebenen. Gemessen wird seine Flaeche gegen die Nachbarn, nicht sein '
            + 'Innenleben: das waere eine Pruefung einer fremden Bibliothek gegen die Layoutregeln '
            + 'dieses Produkts.',
    },
];

/**
 * Bewusste Overlays, die per Bauart ueber anderem Text liegen.
 *
 * Sie brauchen keine Sonderregel im Code (die Ebenen-Regel oben deckt sie ab),
 * aber sie gehoeren benannt: ein Leser des Artefakts soll wissen, WELCHE
 * Ueberlagerungen dieses Produkt absichtlich zeigt.
 */
export const DELIBERATE_OVERLAYS = [
    { selector: '[data-testid="atlas-search-results"]', reason: 'Das Suchfenster liegt ueber der Kommandozeile.' },
    { selector: '[data-testid="atlas-entry"]', reason: 'Der Einstiegsdialog liegt ueber der Editorflaeche.' },
    { selector: '[data-testid="atlas-why"]', reason: 'Die Frage nach dem Warum fuellt die leere Editorflaeche.' },
    { selector: '[data-testid="codeatlas-evidence-popover"]', reason: 'Ein Beleg klappt ueber die Zeile auf, zu der er gehoert.' },
    { selector: '[data-testid="atlas-help"]', reason: 'Die Hilfeseite liegt ueber der Editorflaeche.' },
];

/**
 * Vier Flaechen, die bis W8 in dieser Liste standen und es nicht mehr tun.
 *
 * Der Flow-Erklaerer, das Antwort-Panel, der BUG-Assistent und die
 * Aenderungsansicht lagen ueber der Editorflaeche; seit W8 sind sie Reiter des
 * Erklaeren-Bereichs und liegen NEBEN dem Reader, nicht davor. Sie hier
 * stehenzulassen waere eine dokumentierte Ausnahme fuer etwas, das keine mehr
 * braucht, und ein Leser des Artefakts wuerde eine Ueberlagerung vermuten, die
 * es nicht mehr gibt. Der Satz bleibt trotzdem stehen, weil die Liste eine
 * Aussage ueber die Oberflaeche ist und diese Aussage sich geaendert hat.
 */
export const OVERLAYS_TURNED_INTO_ZONES = [
    '[data-testid="atlas-flow-overlay"]',
    '[data-testid="atlas-chat"]',
    '[data-testid="atlas-bugwizard"]',
    '[data-testid="atlas-impact"]',
];

/**
 * Die Messung, als Zeichenkette fuer `page.evaluate`.
 *
 * Sie steht als eine Funktion da, weil sie im Browser laeuft und dort nichts
 * aus diesem Modul sieht.
 */
const PROBE = (exclusions) => {
    const excluded = (node) => exclusions.some((selector) => node.closest(selector) !== null);

    const identify = (node) => {
        const testId = node.getAttribute('data-testid');
        if (testId !== null && testId.length > 0) {
            return `[${testId}]`;
        }
        const classes = (node.getAttribute('class') ?? '').split(/\s+/).filter(Boolean);
        return classes.length > 0 ? `.${classes[0]}` : node.tagName.toLowerCase();
    };

    const path = (node) => {
        const parts = [];
        let current = node;
        for (let i = 0; i < 4 && current !== null && current !== document.body; i += 1) {
            parts.unshift(identify(current));
            current = current.parentElement;
        }
        return parts.join(' > ');
    };

    const ownText = (node) => {
        let text = '';
        for (const child of node.childNodes) {
            if (child.nodeType === Node.TEXT_NODE) {
                text += child.nodeValue ?? '';
            }
        }
        return text.replace(/\s+/g, ' ').trim();
    };

    const viewport = { width: window.innerWidth, height: window.innerHeight };
    const candidates = [];
    const layerKeys = new Set();

    /**
     * Was von einem Rechteck nach allen Kaesten darueber uebrig bleibt.
     *
     * Jeder Vorfahre mit `overflow != visible` schneidet auf seiner Achse, und
     * zuletzt schneidet das Fenster. Das Ergebnis ist die Flaeche, die ein
     * Leser wirklich sieht.
     */
    const visibleRectOf = (node) => {
        const rect = node.getBoundingClientRect();
        let left = rect.left;
        let top = rect.top;
        let right = rect.right;
        let bottom = rect.bottom;
        for (
            let current = node.parentElement;
            current !== null && current !== document.documentElement;
            current = current.parentElement
        ) {
            const style = window.getComputedStyle(current);
            if (style.overflowX === 'visible' && style.overflowY === 'visible') {
                continue;
            }
            const box = current.getBoundingClientRect();
            if (style.overflowX !== 'visible') {
                left = Math.max(left, box.left);
                right = Math.min(right, box.right);
            }
            if (style.overflowY !== 'visible') {
                top = Math.max(top, box.top);
                bottom = Math.min(bottom, box.bottom);
            }
        }
        left = Math.max(left, 0);
        top = Math.max(top, 0);
        right = Math.min(right, viewport.width);
        bottom = Math.min(bottom, viewport.height);
        return { x: left, y: top, width: right - left, height: bottom - top };
    };

    for (const node of document.body.querySelectorAll('*')) {
        if (excluded(node)) {
            continue;
        }
        const text = ownText(node);
        if (text.length === 0) {
            continue;
        }
        const style = window.getComputedStyle(node);
        if (style.display === 'none' || style.visibility === 'hidden' || Number(style.opacity) < 0.05) {
            continue;
        }
        const rect = node.getBoundingClientRect();
        if (rect.width <= 0 || rect.height <= 0) {
            continue;
        }
        // Weggescrollt oder ausserhalb des Fensters heisst: der Leser sieht es
        // gerade nicht. Es dort zu messen waere eine Aussage ueber einen
        // Bildschirm, den es nicht gibt.
        const visible = visibleRectOf(node);
        if (visible.width <= 0.5 || visible.height <= 0.5) {
            continue;
        }

        // Die Ebene: der naechste positionierte Vorfahre, sich selbst eingeschlossen.
        let layerNode = null;
        for (let current = node; current !== null && current !== document.body; current = current.parentElement) {
            const position = window.getComputedStyle(current).position;
            if (position === 'absolute' || position === 'fixed' || position === 'sticky') {
                layerNode = current;
                break;
            }
        }
        const layer = layerNode === null ? 'document' : path(layerNode);
        layerKeys.add(layer);

        candidates.push({
            node,
            layer,
            text: text.slice(0, 80),
            path: path(node),
            rect: { x: rect.x, y: rect.y, width: rect.width, height: rect.height },
            visible,
        });
    }

    // ------------------------------------------------------ (a) Ueberlagerung
    const overlaps = [];
    for (let i = 0; i < candidates.length; i += 1) {
        for (let j = i + 1; j < candidates.length; j += 1) {
            const a = candidates[i];
            const b = candidates[j];
            if (a.layer !== b.layer) {
                continue;
            }
            if (a.node.contains(b.node) || b.node.contains(a.node)) {
                continue;
            }
            const overlapX = Math.min(a.visible.x + a.visible.width, b.visible.x + b.visible.width)
                - Math.max(a.visible.x, b.visible.x);
            const overlapY = Math.min(a.visible.y + a.visible.height, b.visible.y + b.visible.height)
                - Math.max(a.visible.y, b.visible.y);
            if (overlapX > 1 && overlapY > 1) {
                overlaps.push({
                    layer: a.layer,
                    a: { path: a.path, text: a.text, rect: a.visible },
                    b: { path: b.path, text: b.text, rect: b.visible },
                    overlapX: Number(overlapX.toFixed(2)),
                    overlapY: Number(overlapY.toFixed(2)),
                });
            }
        }
    }

    // --------------------------------- (c) Der Hinweis auf mehr Inhalt
    /**
     * Ob ein scrollender Kasten an dieser Kante sagt, dass es weitergeht.
     *
     * Fuenf Formen, und jede davon ist im Bild zu sehen. Die Reihenfolge ist
     * die ihrer Genauigkeit: eine Marke nennt ihre Kante selbst, ein Streifen
     * mit Verlauf liegt an einer bestimmten Kante, eine Maske und ein
     * mitrollender Hintergrund gelten fuer den ganzen Kasten, und eine
     * Bildlaufleiste zaehlt nur, wenn sie wirklich Platz einnimmt (auf dieser
     * Plattform tut sie das nicht, sie liegt ueberlagernd und ist im
     * Ruhezustand unsichtbar).
     */
    const showsMore = (box, edge) => {
        const marks = [
            ...box.querySelectorAll('[data-scroll-hint]'),
            ...(box.parentElement === null
                ? []
                : [...box.parentElement.children].filter((child) =>
                    child !== box && child.hasAttribute('data-scroll-hint'))),
        ];
        for (const mark of marks) {
            const style = window.getComputedStyle(mark);
            const rect = mark.getBoundingClientRect();
            const seen = rect.width > 0 && rect.height > 0
                && style.display !== 'none' && style.visibility !== 'hidden'
                && Number(style.opacity) >= 0.05;
            const says = (mark.getAttribute('data-scroll-hint') ?? '').split(/\s+/);
            if (seen && (says.includes(edge) || says.includes('any'))) {
                return { how: 'mark', detail: mark.getAttribute('data-scroll-hint') ?? '' };
            }
        }
        const style = window.getComputedStyle(box);
        const masks = [style.maskImage, style.webkitMaskImage]
            .filter((value) => typeof value === 'string' && value.length > 0 && value !== 'none');
        if (masks.length > 0) {
            return { how: 'mask', detail: masks[0].slice(0, 60) };
        }
        /*
         * Ein Verlauf, der ueber dieser Kante liegt: ein sichtbares Geschwister
         * im selben Rahmen, das die Kante beruehrt und einen Farbverlauf malt.
         * Die Tab-Leiste macht es so (zwei Streifen an den Raendern, die nur
         * erscheinen, wenn wirklich etwas herausragt), und diese Form ist
         * genauso ein Hinweis wie eine Marke: der Leser sieht, dass der Text
         * dort nicht endet, sondern weitergeht.
         */
        const boxRect = box.getBoundingClientRect();
        const near = 26;
        for (const other of box.parentElement === null ? [] : box.parentElement.children) {
            if (other === box) {
                continue;
            }
            const otherStyle = window.getComputedStyle(other);
            if (!/gradient/.test(otherStyle.backgroundImage ?? '')) {
                continue;
            }
            if (otherStyle.display === 'none' || otherStyle.visibility === 'hidden'
                || Number(otherStyle.opacity) < 0.05) {
                continue;
            }
            const rect = other.getBoundingClientRect();
            const touches = edge === 'bottom'
                ? Math.abs(rect.bottom - boxRect.bottom) <= near && rect.top < boxRect.bottom
                : edge === 'top'
                    ? Math.abs(rect.top - boxRect.top) <= near && rect.bottom > boxRect.top
                    : edge === 'right'
                        ? Math.abs(rect.right - boxRect.right) <= near && rect.left < boxRect.right
                        : Math.abs(rect.left - boxRect.left) <= near && rect.right > boxRect.left;
            if (touches) {
                return { how: 'gradient-edge', detail: identify(other) };
            }
        }
        if ((style.backgroundAttachment ?? '').includes('local')) {
            return { how: 'scrolling-background', detail: style.backgroundAttachment };
        }
        const border = (axis) => (axis === 'y'
            ? Number.parseFloat(style.borderTopWidth) + Number.parseFloat(style.borderBottomWidth)
            : Number.parseFloat(style.borderLeftWidth) + Number.parseFloat(style.borderRightWidth));
        const bar = edge === 'top' || edge === 'bottom'
            ? box.offsetWidth - box.clientWidth - border('x')
            : box.offsetHeight - box.clientHeight - border('y');
        if (bar > 1) {
            return { how: 'scrollbar', detail: `${Number(bar.toFixed(2))}px` };
        }
        return null;
    };

    /** Die Rechtecke der einzelnen Zeilen des eigenen Textes eines Elements. */
    const lineRects = (node) => {
        const rects = [];
        const range = document.createRange();
        for (const child of node.childNodes) {
            if (child.nodeType !== Node.TEXT_NODE || (child.nodeValue ?? '').trim().length === 0) {
                continue;
            }
            range.selectNodeContents(child);
            rects.push(...range.getClientRects());
        }
        return rects;
    };

    // ------------------------------------------------------- (b) Beschneidung
    const clipped = [];
    const cutting = (style, axis) => {
        const value = axis === 'x' ? style.overflowX : style.overflowY;
        return value === 'hidden' || value === 'clip';
    };
    for (const candidate of candidates) {
        for (const axis of ['x', 'y']) {
            let clipper = null;
            let scroller = null;
            for (
                let current = candidate.node.parentElement;
                current !== null && current !== document.documentElement;
                current = current.parentElement
            ) {
                const style = window.getComputedStyle(current);
                if (cutting(style, axis)) {
                    clipper = current;
                    break;
                }
                if ((axis === 'x' ? style.overflowX : style.overflowY) !== 'visible') {
                    // Ein scrollbarer Kasten schneidet nicht ab: was heraushaengt,
                    // ist erreichbar. Die Suche endet hier trotzdem, denn er
                    // begrenzt, was ein aeusserer Kasten ueberhaupt sehen kann.
                    // Ob er es dem Leser SAGT, fragt (c) gleich darunter.
                    scroller = current;
                    break;
                }
            }
            if (scroller !== null) {
                /*
                 * (c): geht die Kante mitten durch eine Zeile, und steht dort
                 * ein Hinweis? Nur am Anfang des Bildlaufs, also an der fernen
                 * Kante; siehe Kopf.
                 */
                const room = axis === 'y'
                    ? scroller.scrollHeight - scroller.clientHeight
                    : scroller.scrollWidth - scroller.clientWidth;
                const offset = axis === 'y' ? scroller.scrollTop : scroller.scrollLeft;
                if (room > 1 && offset <= 1) {
                    const box = scroller.getBoundingClientRect();
                    const border = window.getComputedStyle(scroller);
                    const far = axis === 'y'
                        ? box.bottom - Number.parseFloat(border.borderBottomWidth)
                        : box.right - Number.parseFloat(border.borderRightWidth);
                    for (const line of lineRects(candidate.node)) {
                        const start = axis === 'y' ? line.top : line.left;
                        const end = axis === 'y' ? line.bottom : line.right;
                        const shown = far - start;
                        const gone = end - far;
                        /*
                         * Ein Viertel der Zeile auf jeder Seite, mindestens
                         * zwei Pixel. Der Kasten einer Zeile ist hoeher als
                         * ihre Schrift (halber Zeilendurchschuss oben und
                         * unten), und ein Schnitt, der nur diesen Rand
                         * erwischt, zeigt keine halbe Zeile, sondern gar
                         * keine. Gefragt ist der Fall, den ein Leser als
                         * abgeschnittenen Satz sieht.
                         */
                        const bite = Math.max(2, (end - start) * 0.25);
                        if (shown <= bite || gone <= bite) {
                            continue;
                        }
                        const hint = showsMore(scroller, axis === 'y' ? 'bottom' : 'right');
                        if (hint === null) {
                            clipped.push({
                                kind: 'cut-without-hint',
                                axis,
                                element: {
                                    path: candidate.path,
                                    text: candidate.text,
                                    rect: candidate.rect,
                                },
                                container: {
                                    path: path(scroller),
                                    rect: { x: box.x, y: box.y, width: box.width, height: box.height },
                                    hidden: Number(room.toFixed(2)),
                                },
                                overflowPx: Number(gone.toFixed(2)),
                                shownPx: Number(shown.toFixed(2)),
                            });
                        }
                        // Hoechstens eine Meldung je Element und Achse: die
                        // Kante geht durch genau eine Zeile, und die ist
                        // gefunden.
                        break;
                    }
                }
            }
            if (clipper === null) {
                continue;
            }
            const box = clipper.getBoundingClientRect();
            const near = candidate.rect;
            const onScreen = near.x + near.width > box.x && near.x < box.x + box.width
                && near.y + near.height > box.y && near.y < box.y + box.height;
            if (!onScreen) {
                continue;
            }
            const over = axis === 'x'
                ? Math.max(box.x - near.x, (near.x + near.width) - (box.x + box.width))
                : Math.max(box.y - near.y, (near.y + near.height) - (box.y + box.height));
            /*
             * Zwei verschiedene Schwellen, weil die Achsen verschieden sind.
             * Senkrecht ist der Kasten einer Zeile immer etwas hoeher als ihre
             * Schrift; waagerecht sind abgeschnittene Pixel Buchstaben, ausser
             * der Kasten kuerzt selbst mit drei Punkten.
             */
            const ellipsis = window.getComputedStyle(clipper).textOverflow === 'ellipsis'
                || window.getComputedStyle(candidate.node).textOverflow === 'ellipsis';
            const threshold = axis === 'y'
                ? Math.max(4, near.height * 0.35)
                : (ellipsis ? Number.POSITIVE_INFINITY : 2);
            if (over > threshold) {
                clipped.push({
                    kind: 'clipped',
                    axis,
                    element: { path: candidate.path, text: candidate.text, rect: near },
                    container: { path: path(clipper), rect: { x: box.x, y: box.y, width: box.width, height: box.height } },
                    overflowPx: Number(over.toFixed(2)),
                });
            }
        }
    }

    return {
        candidates: candidates.length,
        layers: [...layerKeys].sort(),
        overlaps,
        clipped,
        viewport,
    };
};

/** Die Messung an dieser Stelle der Strecke. */
export async function measureReadability(page, exclusions = READABILITY_EXCLUSIONS) {
    return page.evaluate(PROBE, exclusions.map((entry) => entry.selector));
}

/**
 * Jeden wirklich scrollbaren Bereich ans Ende fahren.
 *
 * Zurueck kommt, was gescrollt wurde, mit einem Namen je Bereich. Die Namen
 * sind die Testmarken oder die erste CSS-Klasse; das reicht, um zwei Bereiche
 * zu unterscheiden, und ist stabil ueber die Halte hinweg.
 */
export async function scrollRegionsToEnd(page, exclusions = READABILITY_EXCLUSIONS) {
    return page.evaluate((selectors) => {
        const excluded = (node) => selectors.some((selector) => node.closest(selector) !== null);
        const identify = (node) => {
            const testId = node.getAttribute('data-testid');
            if (testId !== null && testId.length > 0) {
                return testId;
            }
            const classes = (node.getAttribute('class') ?? '').split(/\s+/).filter(Boolean);
            return classes.length > 0 ? classes[0] : node.tagName.toLowerCase();
        };
        const scrolled = [];
        for (const node of document.body.querySelectorAll('*')) {
            if (excluded(node)) {
                continue;
            }
            const rect = node.getBoundingClientRect();
            if (rect.width <= 0 || rect.height <= 0) {
                continue;
            }
            const style = window.getComputedStyle(node);
            const scrollableY = (style.overflowY === 'auto' || style.overflowY === 'scroll')
                && node.scrollHeight > node.clientHeight + 1;
            const scrollableX = (style.overflowX === 'auto' || style.overflowX === 'scroll')
                && node.scrollWidth > node.clientWidth + 1;
            if (!scrollableY && !scrollableX) {
                continue;
            }
            const before = { top: node.scrollTop, left: node.scrollLeft };
            if (scrollableY) {
                node.scrollTop = node.scrollHeight;
            }
            if (scrollableX) {
                node.scrollLeft = node.scrollWidth;
            }
            scrolled.push({
                name: identify(node),
                axis: scrollableY && scrollableX ? 'both' : scrollableY ? 'y' : 'x',
                before,
                after: { top: node.scrollTop, left: node.scrollLeft },
                scrollHeight: node.scrollHeight,
                clientHeight: node.clientHeight,
                scrollWidth: node.scrollWidth,
                clientWidth: node.clientWidth,
            });
        }
        return scrolled;
    }, exclusions.map((entry) => entry.selector));
}

/**
 * Alle Bereiche wieder an den Anfang, damit der naechste Schritt oben beginnt.
 *
 * Die ausgenommenen Flaechen bleiben unberuehrt: dem Editor seinen Bildlauf von
 * aussen zu setzen hiesse, an einer Bibliothek zu drehen, die ihren eigenen
 * Zustand darueber fuehrt.
 */
export async function resetScroll(page, exclusions = READABILITY_EXCLUSIONS) {
    await page.evaluate((selectors) => {
        const excluded = (node) => selectors.some((selector) => node.closest(selector) !== null);
        for (const node of document.body.querySelectorAll('*')) {
            if (excluded(node)) {
                continue;
            }
            if (node.scrollTop !== 0) {
                node.scrollTop = 0;
            }
            if (node.scrollLeft !== 0) {
                node.scrollLeft = 0;
            }
        }
    }, exclusions.map((entry) => entry.selector));
}

// ---------------------------------------------------------------- (e) Tooltips

/**
 * Was ein offener Tooltip nicht verdecken darf, als Daten und mit Grund.
 *
 * WORTGLEICH mit `HINT_PROTECTED` in src/ui/tooltip/tooltip-model.ts, und das
 * ist Absicht: die Oberflaeche platziert nach dieser Liste, dieser Lauf misst
 * nach derselben. Sie hier zu erweitern, ohne sie dort zu erweitern, hiesse
 * eine Zusicherung zu pruefen, die das Produkt gar nicht gibt.
 */
export const TOOLTIP_PROTECTED = [
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

/**
 * Alle Ausloeser, die diese Seite gerade traegt.
 *
 * Zurueck kommt eine Liste von Griffen, an denen der Lauf sie einzeln oeffnen
 * kann: die laufende Nummer (stabil ueber einen Halt), der Name, den die
 * Oberflaeche dem Tooltip gegeben hat, und der Satz. Nur SICHTBARE, denn ein
 * Ausloeser ohne Rechteck laesst sich weder beruehren noch fokussieren, und ein
 * Bericht ueber ihn waere ein Bericht ueber einen Bildschirm, den es nicht gibt.
 */
export async function tooltipTriggers(page) {
    return page.evaluate(() => {
        const out = [];
        let index = 0;
        for (const node of document.querySelectorAll('[data-hint]')) {
            const rect = node.getBoundingClientRect();
            const style = window.getComputedStyle(node);
            const seen = rect.width > 0 && rect.height > 0
                && style.display !== 'none' && style.visibility !== 'hidden'
                && Number(style.opacity) >= 0.05
                && rect.bottom > 0 && rect.top < window.innerHeight
                && rect.right > 0 && rect.left < window.innerWidth;
            node.setAttribute('data-hint-index', String(index));
            if (seen) {
                out.push({
                    index,
                    name: node.getAttribute('data-hint-name') ?? '',
                    text: node.getAttribute('data-hint') ?? '',
                    tag: node.tagName.toLowerCase(),
                    focusable: node.tabIndex >= 0,
                    rect: {
                        x: Math.round(rect.x),
                        y: Math.round(rect.y),
                        width: Math.round(rect.width),
                        height: Math.round(rect.height),
                    },
                });
            }
            index += 1;
        }
        return out;
    });
}

/**
 * Einen Tooltip oeffnen und messen, was er verdeckt.
 *
 * `how` ist `pointer` oder `keyboard`, und der Unterschied ist die Zusicherung
 * aus AC1: ein Tooltip, den nur die Maus oeffnet, ist keiner. Beide Wege gehen
 * durch dieselben Rueckrufe der Oberflaeche, also misst der Lauf zweimal
 * dasselbe Versprechen und nicht zwei verschiedene.
 *
 * Zurueck kommt, ob der Kasten wirklich dasteht, wo er steht, und die Liste der
 * geschuetzten Flaechen, die er schneidet. Leer ist die bestandene Antwort.
 */
export async function measureTooltip(page, index, how = 'pointer') {
    return page.evaluate(({ at, mode, selectors }) => {
        const trigger = document.querySelector(`[data-hint-index="${at}"]`);
        if (trigger === null) {
            return { opened: false, reason: 'trigger gone' };
        }
        if (mode === 'keyboard') {
            trigger.focus?.();
            trigger.dispatchEvent(new FocusEvent('focusin', { bubbles: true }));
        } else {
            const rect = trigger.getBoundingClientRect();
            trigger.dispatchEvent(new MouseEvent('mouseover', {
                bubbles: true,
                clientX: rect.left + rect.width / 2,
                clientY: rect.top + rect.height / 2,
            }));
        }
        return {
            opened: true,
            described: trigger.getAttribute('aria-describedby') ?? '',
        };
    }, { at: index, mode: how });
}

/** Die Selektoren der geschuetzten Flaechen, in einem Stueck. */
const protectedSelector = () => TOOLTIP_PROTECTED.map((entry) => entry.selector).join(', ');

/**
 * Was der gerade offene Tooltip verdeckt.
 *
 * Getrennt vom Oeffnen, weil zwischen beidem ein Bild liegen muss: der Kasten
 * misst sich selbst, bevor er sich stellt (siehe src/ui/tooltip/Hint.tsx), und
 * ein Lauf, der sofort nachschaut, sieht die erste, unsichtbare Lage.
 */
export async function tooltipCover(page, index) {
    return page.evaluate(({ at, protectSelector }) => {
        const box = document.querySelector('[data-testid="atlas-hint"]');
        if (box === null) {
            return { open: false, covers: [], rect: null, name: '' };
        }
        const style = window.getComputedStyle(box);
        const rect = box.getBoundingClientRect();
        if (style.visibility === 'hidden' || rect.width <= 0 || rect.height <= 0) {
            return { open: false, covers: [], rect: null, name: box.getAttribute('data-hint-for') ?? '' };
        }
        const trigger = document.querySelector(`[data-hint-index="${at}"]`);
        const overlap = (other) => {
            const width = Math.min(rect.right, other.right) - Math.max(rect.left, other.left);
            const height = Math.min(rect.bottom, other.bottom) - Math.max(rect.top, other.top);
            return width > 1 && height > 1 ? Math.round(width * height) : 0;
        };
        const covers = [];
        const candidates = [...document.querySelectorAll(protectSelector)];
        if (trigger !== null) {
            candidates.push(trigger);
        }
        for (const node of candidates) {
            if (node === box || box.contains(node) || node.contains(box)) {
                continue;
            }
            const other = node.getBoundingClientRect();
            if (other.width <= 0 || other.height <= 0) {
                continue;
            }
            const nodeStyle = window.getComputedStyle(node);
            if (nodeStyle.display === 'none' || nodeStyle.visibility === 'hidden') {
                continue;
            }
            const area = overlap(other);
            if (area > 0) {
                covers.push({
                    what: node === trigger
                        ? 'its own trigger'
                        : node.getAttribute('data-hint-keep')
                        ?? node.getAttribute('data-testid')
                        ?? node.tagName.toLowerCase(),
                    area,
                });
            }
        }
        return {
            open: true,
            name: box.getAttribute('data-hint-for') ?? '',
            side: box.getAttribute('data-side') ?? '',
            text: (box.textContent ?? '').replace(/\s+/g, ' ').trim().slice(0, 90),
            rect: {
                x: Math.round(rect.x),
                y: Math.round(rect.y),
                width: Math.round(rect.width),
                height: Math.round(rect.height),
            },
            covers,
        };
    }, { at: index, protectSelector: protectedSelector() });
}

/** Alles wieder zu: der naechste Halt soll nicht mit einem offenen Kasten beginnen. */
export async function closeTooltips(page) {
    await page.evaluate(() => {
        for (const node of document.querySelectorAll('[data-hint][data-hint-open="true"]')) {
            node.dispatchEvent(new MouseEvent('mouseout', { bubbles: true }));
            node.blur?.();
            node.dispatchEvent(new FocusEvent('focusout', { bubbles: true }));
        }
        if (document.activeElement instanceof HTMLElement) {
            document.activeElement.blur();
        }
    });
}

/**
 * Wie viele native Tooltips diese Seite noch traegt, die etwas ERKLAEREN.
 *
 * Die Zusicherung aus AC1, als Zahl. Gezaehlt wird nicht "hat ein title",
 * sondern "hat einen title, der mehr sagt als der sichtbare Text": ein
 * Attribut, das nur wiederholt, was danebensteht, verdeckt zwar auch etwas, ist
 * aber ein anderer Fehler und faellt in W8b ersatzlos weg. Dieselbe Bedingung
 * wie `explainsMore` in src/ui/tooltip/tooltip-model.ts.
 */
export async function nativeTitles(page) {
    return page.evaluate(() => {
        const tidy = (value) => value.replace(/\s+/g, ' ').trim().toLowerCase();
        const out = [];
        for (const node of document.querySelectorAll('[title]')) {
            if (node.closest('.monaco-editor') !== null) {
                continue;
            }
            const said = tidy(node.getAttribute('title') ?? '');
            if (said.length === 0) {
                continue;
            }
            const shown = tidy(node.textContent ?? '');
            if (shown.includes(said)) {
                continue;
            }
            out.push({
                path: node.tagName.toLowerCase()
                    + (node.getAttribute('data-testid') === null
                        ? ''
                        : `[${node.getAttribute('data-testid')}]`),
                title: said.slice(0, 90),
                text: shown.slice(0, 60),
            });
        }
        return out;
    });
}
