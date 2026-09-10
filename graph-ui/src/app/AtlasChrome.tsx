/**
 * Das Chrome: Kopfzeile, Menue, Tabs, Explorer, Breadcrumb, Reader-Flaeche,
 * Kommandozeile, Statusleiste. Vorlage ist design/design.png.
 *
 * Rein darstellend, und das ist der Punkt: der Reader kommt als `children`
 * herein, nicht als Import. So laesst sich das ganze Chrome in jsdom pruefen,
 * ohne Monaco zu laden, und der Beweislauf im Browser prueft danach genau das,
 * was jsdom nicht kann, naemlich den Editor selbst.
 *
 * Die Menuezeile zeichnet nur, was etwas tut.
 *
 * Bis W3 war ein Menuepunkt ohne Verdrahtung ein Knopf mit einem `title`, der
 * sagte, dass er nichts tut; seit W3 galt das Punkt fuer Punkt statt pauschal.
 * Seit W7a (Nutzerauftrag 2026-08-29) gilt es gar nicht mehr: ein Punkt ohne
 * Verdrahtung wird NICHT gezeichnet. Ein Tooltip macht aus einer Attrappe keine
 * Auskunft, sondern eine Attrappe mit Fussnote, und ein Menue, das sich
 * anklicken laesst und schweigt, ist die Sorte stille Luege, gegen die dieses
 * Produkt gebaut ist (PLAN.md Abschnitt 6, Ehrlichkeitsregeln).
 *
 * Das ist hier die Notbremse und nicht der Normalfall: welche Punkte es gibt,
 * entscheidet `messages.menu.items`, und dass jeder davon einen Buchstaben in
 * WIRED_MENU_SHORTCUTS hat, prueft src/app/shortcuts.test.ts. Faellt eine
 * Verdrahtung trotzdem einmal aus, fehlt der Punkt, statt stumm dazustehen.
 *
 * Die Kommandozeile bekommt ihren Hinweis und ihren `title` von aussen, weil
 * das, was sie tut, nicht mehr hier entschieden wird.
 */
import type { CSSProperties, JSX, KeyboardEvent, ReactNode, RefObject } from 'react';
import { useCallback, useEffect, useLayoutEffect, useRef, useState } from 'react';
import { createPortal } from 'react-dom';
import AtlasTree from './AtlasTree';
import type { AtlasTreeProps } from './AtlasTree';
import { messages } from '../i18n/messages';
import type { CommandExample } from '../search/command-examples';
import Hint from '../ui/tooltip/Hint';
import { LAYOUT_DEFAULT } from '../layout/layout-model';
import Splitter from '../layout/Splitter';
import { workspaceStrings } from './workspace-strings';
import type { Workspace, Guidance } from './workspace-strings';

/** Ein Menuepunkt: sein Buchstaben-Kuerzel und der Rest des Wortes. */
export interface MenuItem {
    key: string;
    rest: string;
}

/**
 * Was ein verdrahteter Menuepunkt tut.
 *
 * `state` faerbt ihn: `on` heisst, die Flaeche dahinter ist zu sehen, `off`
 * heisst, es gibt sie und sie ist gerade zu. Einen dritten Wert fuer "gibt es
 * nicht" braucht es seit W7a nicht mehr: was es nicht gibt, steht nicht in der
 * Zeile.
 */
export interface MenuWiring {
    title: string;
    state: 'on' | 'off';
    onSelect: () => void;
    /**
     * Further entries of the same menu, drawn beside it.
     *
     * A dropdown would be the ordinary shape and it is not the shape here, for
     * one reason: the letter itself is already a verb. `[a]tlas` shows and hides
     * the galaxy on click and on the `a` key, and the W3 proof run turns on that
     * meaning. Turning the item into a trigger that only opens a list would
     * change what a click on it does, which is a change to a proven behaviour
     * made in passing for a second entry's sake. So the extra entries sit next to
     * it as their own small buttons, each with its own key in `data-menu`, and
     * the primary letter keeps meaning what it has always meant.
     */
    extras?: readonly MenuExtra[];
}

/** One further entry of a menu: its own label, its own tooltip, its own act. */
export interface MenuExtra {
    /** Suffix of `data-menu`, so the entry is addressable as `a-why`. */
    key: string;
    label: string;
    title: string;
    onSelect: () => void;
}

/**
 * Der Klammer-Buchstabe eines Etiketts und der Rest davon.
 *
 * Nutzerbefund vom 2026-08-29 (Screenshot der Menuezeile): `[a]tlas` und
 * `[?]help` trugen einen Rahmen, die vier Eintraege dazwischen nur einen
 * gepunkteten Unterstrich, und dadurch sahen sie aus wie Text neben
 * Bedienelementen. Sie sind aber dasselbe: sechs Knoepfe, jeder mit einem
 * Buchstaben, jeder mit einer Handlung dahinter.
 *
 * Die vier Eintraege fuehren ihren Buchstaben bisher IM Etikett (`[w]hy am I
 * here`), weil das Etikett an mehreren Stellen gelesen wird und dort seinen
 * Buchstaben zeigen soll. Damit sie trotzdem dieselbe Gestalt bekommen wie die
 * beiden anderen, wird der Buchstabe hier wieder herausgeloest und in dasselbe
 * `<span class="atlas-menu-key">` gesetzt. Das ist eine Ablesung des Etiketts
 * und keine zweite Quelle: steht keine Klammer darin, bleibt das Etikett ganz
 * stehen, statt dass eine erfundene Klammer erscheint.
 */
export function splitMenuLabel(label: string): MenuItem {
    const match = /^\s*\[([^\]])\](.*)$/.exec(label);
    return match === null ? { key: '', rest: label } : { key: match[1], rest: match[2] };
}

/**
 * Die Menuezeile aus dem Vorbild. Jeder Punkt traegt seinen Buchstaben, und der
 * Buchstabe steht in Klammern, weil ein Terminal keine Unterstreichung hat.
 */
export const MENU_ITEMS: readonly MenuItem[] = messages.menu.items;

/**
 * Der Platzhalter der Kommandozeile, wenn kein Index geladen ist.
 *
 * Bis W8b war er der einzige, und er nannte eine GATTUNG ("type a command or
 * ask the atlas"). Der Nutzer am 2026-08-29: "anstelle von 'type a command or
 * ask the atlas' explizite Beispiele, wie man den Chat nutzt, sonst weiss
 * niemand, wie man es nutzt." Seit W8b nennt die Zeile ein echtes Beispiel mit
 * einem Symbol dieses Projekts (src/search/command-examples.ts); dieser Satz
 * bleibt fuer den Fall, dass noch nichts geladen ist, und das ist der ehrliche
 * Rueckfall: ein Beispiel mit einem erfundenen Namen wuerde behaupten, es gebe
 * dieses Symbol.
 */
export const COMMAND_PLACEHOLDER = messages.command.placeholder;

/** Eine Angabe in der Kopfzeile oder der Statusleiste. */
export interface Chip {
    label: string;
    value: string;
    /** `absent` faerbt die Angabe als Abwesenheit statt als Zahl. */
    state?: 'ok' | 'absent' | 'plain';
}

/** Ein offener Tab. */
export interface TabDescriptor {
    path: string;
    name: string;
    active: boolean;
}

export interface AtlasChromeProps {
    projectSwitcher?: ReactNode;
    onOpenBrowserAi?: () => void;
    chatOpen?: boolean;
    chatDock?: ReactNode;
    /** Persistent code context, shared by the Explore sidebar and chat column. */
    selectionInspector?: ReactNode;
    readerActions?: ReactNode;
    onOpenSystem?: () => void;
    daemonState?: 'connected' | 'disconnected' | 'checking';
    globalOverlay?: ReactNode;
    workspace?: Workspace;
    onWorkspaceChange?: (workspace: Workspace) => void;
    workspacePanel?: ReactNode;
    workspaceStatus?: ReactNode;
    guidance?: Guidance;
    onGuidanceChange?: (guidance: Guidance) => void;
    /** Versions-Chip, zur Buildzeit injiziert. Nur die Fassung, ohne Zusatz. */
    version: string;
    /**
     * Der Zusatz neben dem Chip: `dirty`, wenn beim Bauen etwas uncommitted war.
     *
     * Ein eigenes Element und nicht ein Anhaengsel im Chip, weil es eine andere
     * Aussage ist: der Chip nennt die Fassung, der Zusatz den Zustand des
     * Baums, aus dem sie gebaut wurde. Zusammengeschrieben liest sich das wie
     * eine Fassung namens "1.0.0-dirty", die es als Release nie gibt. Fehlt der
     * Zusatz, steht dort nichts: ein leeres Element waere ein Platz, an dem
     * jemand eine Angabe vermutet.
     */
    buildSuffix?: string;
    chips: Chip[];
    tabs: TabDescriptor[];
    onSelectTab: (path: string) => void;
    onCloseTab: (path: string) => void;
    tree: AtlasTreeProps;
    /** Pfadsegmente der offenen Datei. Leer heisst: keine Datei offen. */
    breadcrumb: string[];
    /** Die Editor-Flaeche. Kommt von aussen, damit dieses Modul Monaco nicht kennt. */
    children: ReactNode;
    /** Ehrliche Zeile unter dem Editor, wenn nicht die ganze Datei angekommen ist. */
    truncationNote: string;
    /**
     * Die Coverage-Notiz UEBER dem Editor, wenn der Index diese Datei nur zum
     * Teil kennt.
     *
     * Ueber und nicht unter dem Text, und das ist der ganze Unterschied: die
     * Kappungszeile sagt "hier hoert das Geladene auf" und gehoert ans Ende,
     * die Coverage-Notiz sagt "was du gleich liest, ist im Graphen
     * unvollstaendig" und muss vor dem ersten gelesenen Zeichen dastehen.
     */
    coverageNote?: { state: string; text: string } | undefined;
    /**
     * Der Erklaeren-Bereich, unter dem Reader und ueber der Kommandozeile.
     *
     * Seit W8 EIN Platz statt dreier. Bis dahin standen hier zwei Aufnahmen
     * nebeneinander, `tour` und `chat`, und der Flow-Erklaerer lag als drittes
     * ueber dem Editor. Der Nutzerbefund vom 2026-08-29 zeigte alle drei
     * gleichzeitig, uebereinander und angeschnitten. Was einander ersetzt, teilt
     * sich jetzt einen Platz; welcher Reiter gilt, entscheidet App.tsx, und was
     * dieser Bereich ist, steht in src/layout/ExplainZone.tsx.
     *
     * Wie der Reader kommt er als Knoten von aussen herein: dieses Modul soll
     * die fuenf Flaechen so wenig kennen wie den Editor.
     */
    explain?: ReactNode;
    /**
     * Die rechte Spalte. Kommt von aussen, wie der Reader, und aus demselben
     * Grund: dieses Modul soll das Panel so wenig kennen wie den Editor.
     *
     * Fehlt sie, gibt es keine dritte Spalte. Eine leere Spalte zu reservieren
     * waere die Behauptung, es gaebe dort etwas, das gerade nichts sagt.
     */
    twin?: ReactNode;
    /**
     * Die Galaxie, unter dem Twin in derselben Spalte.
     *
     * Warum unter dem Twin und nicht in der Mitte: die Mitte ist der Reader,
     * und ein Panel, das ihn verdraengt, macht aus zwei gleichzeitigen
     * Ansichten ein Umschalten. Die rechte Spalte ist ausserdem die einzige
     * Stelle, an der das Chrome dafuer nichts umbauen muss.
     */
    galaxy?: ReactNode;
    /**
     * Die Karte des lokalen Modells, ganz oben in derselben rechten Spalte.
     *
     * Oben und nicht unten, und das ist der einzige Grund, aus dem sie ueberhaupt
     * eine feste Stelle hat: sie ist zwei Zeilen hoch, solange nichts laeuft, und
     * sie beantwortet die Frage "redet dieses Fenster gerade mit einem Modell".
     * Diese Frage will man beantwortet sehen, bevor man liest, nicht nachdem man
     * an zwei Panels vorbeigescrollt ist.
     */
    llm?: ReactNode;
    /**
     * Die drei Masse, die das Raster braucht.
     *
     * Nur drei und nicht vier: die Hoehe des Erklaeren-Bereichs traegt der
     * Bereich selbst, weil nur er weiss, ob er gerade eingeklappt ist. Die
     * Angabe kommt als CSS-Variable ins Raster und nicht als `grid-template`
     * aus JavaScript, damit die Regeln in terminal.css stehen bleiben, wo alle
     * anderen auch stehen: eine Spaltenbreite, die in zwei Dateien entsteht,
     * ist eine Spaltenbreite, die in zwei Dateien auseinanderlaeuft.
     */
    zones?: { leftWidth: number; rightWidth: number; twinHeight: number };
    /** Der Griff zwischen Explorer und Mitte. */
    splitLeft?: ReactNode;
    /** Der Griff zwischen Reader und Erklaeren-Bereich. */
    splitExplain?: ReactNode;
    /** Der Griff zwischen Mitte und rechter Spalte. */
    splitRight?: ReactNode;
    /** Der Griff zwischen Twin und Graph. */
    splitTwin?: ReactNode;
    /** Verdrahtete Menuepunkte, je Buchstabe. Was fehlt, sagt weiter nichts zu tun. */
    menus?: Partial<Record<string, MenuWiring>>;
    commandValue: string;
    onCommandChange: (value: string) => void;
    /**
     * Tasten der Kommandozeile, bevor das Chrome sie sieht.
     *
     * Wer hier `preventDefault` ruft, hat die Taste verbraucht; das Chrome
     * laesst sie dann in Ruhe. So bleibt Escape zum Verlassen des Feldes da,
     * solange niemand anderes Escape braucht.
     */
    onCommandKeyDown?: (event: KeyboardEvent<HTMLInputElement>) => void;
    /**
     * Ein Griff an das Eingabefeld, fuer die eine Sache, die nur es kann.
     *
     * Der BUG-Assistent bietet "Change symbol" an und schickt den Leser damit in
     * dieselbe Suche, die die Kommandozeile fuehrt. Ohne diesen Griff muesste
     * App.tsx das Feld im DOM suchen, um ihm den Fokus zu geben, und damit
     * waere eine zweite Stelle im Programm, die weiss, wie das Chrome aufgebaut
     * ist. Der Griff bewegt nichts und liest nichts; er zeigt nur auf das Feld.
     */
    commandInputRef?: RefObject<HTMLInputElement | null>;
    /** Das Fenster ueber der Kommandozeile, wenn eines offen ist. */
    commandOverlay?: ReactNode;
    /** Was rechts in der Kommandozeile steht. */
    commandHint: string;
    /**
     * Der Platzhalter, wenn die Zeile leer ist. Ohne Angabe die Gattung.
     *
     * Er kommt von aussen, weil er einen Namen aus dem geladenen Index traegt
     * und dieses Chrome den Index nicht kennt. Dieselbe Trennung wie ueberall
     * hier: die Anwendung weiss, was drinsteht, und das Chrome, wie es aussieht.
     */
    commandPlaceholder?: string;
    /**
     * Die anklickbaren Beispiele ueber der leeren Zeile.
     *
     * Sie erscheinen, solange die Zeile den Fokus hat UND leer ist, und
     * verschwinden, sobald getippt wird: was sie zeigen, ist die Frage "wie
     * benutzt man das", und die stellt niemand mehr, waehrend er schreibt.
     */
    commandExamples?: readonly CommandExample[];
    /** Ein Beispiel in die Zeile schreiben. Nicht abschicken: man soll es aendern koennen. */
    onCommandExample?: (text: string) => void;
    status: Chip[];
}

function ChipView({ chip }: { chip: Chip }): JSX.Element {
    return (
        <span className="atlas-chip" data-state={chip.state ?? 'plain'} data-chip={chip.label}>
            {chip.label} <b>{chip.value}</b>
        </span>
    );
}

/**
 * Die Tab-Leiste, die scrollt statt zu kollidieren.
 *
 * Nutzerfeedback vom 2026-08-29: bei vielen offenen Dateien wuchs die Leiste in
 * die Nachbarbereiche und die Beschriftungen ueberlagerten sich. Vier
 * Entscheidungen, und keine davon ist Kosmetik:
 *
 * 1. **Ein Tab schrumpft nicht.** Das Kollidieren war die Vorgabe von Flexbox:
 *    ein Kind mit `flex-shrink: 1` gibt Breite her, bis sein Inhalt heraushaengt.
 *    Die Leiste laesst ihre Kinder jetzt stehen und scrollt selbst (CSS).
 * 2. **Das Rad scrollt waagerecht.** Ueber einer waagerechten Leiste gibt es
 *    keine senkrechte Bewegung; eine Radumdrehung dort meint diese Leiste, und
 *    ein Mausrad ohne waagerechte Achse waere sonst kein Bedienweg.
 * 3. **Ziehen ist der zweite Weg.** Trackpads koennen waagerecht wischen, Maeuse
 *    oft nicht, und eine Bildlaufleiste waere in einer 30 Pixel hohen Zeile
 *    kaum zu treffen. Gezogen wird mit gedrueckter Maustaste; ein Zug unter
 *    vier Pixeln bleibt ein Klick, sonst waere jeder Tabwechsel ein Zug.
 * 4. **Der aktive Tab wird geholt.** Wer eine Datei oeffnet, will ihren Tab
 *    sehen, auch wenn er ausserhalb liegt.
 */
function TabBar(props: {
    tabs: TabDescriptor[];
    onSelectTab: (path: string) => void;
    onCloseTab: (path: string) => void;
}): JSX.Element {
    const scroller = useRef<HTMLDivElement | null>(null);
    const [overflow, setOverflow] = useState({ left: false, right: false, any: false });
    const [dragging, setDragging] = useState(false);
    const drag = useRef<{ startX: number; startScroll: number; moved: number } | null>(null);
    const dragged = useRef(false);
    const activePath = props.tabs.find((tab) => tab.active)?.path ?? '';

    const measure = useCallback(() => {
        const node = scroller.current;
        if (node === null) {
            return;
        }
        // Ein Pixel Toleranz: der Bildlauf endet auf Bruchteilen, und ein
        // Indikator, der bei 0.4 Pixeln Rest noch leuchtet, luegt.
        const room = node.scrollWidth - node.clientWidth;
        setOverflow({
            left: node.scrollLeft > 1,
            right: node.scrollLeft < room - 1,
            any: room > 1,
        });
    }, []);

    useEffect(() => {
        measure();
        const node = scroller.current;
        if (node === null || typeof ResizeObserver === 'undefined') {
            return;
        }
        const observer = new ResizeObserver(measure);
        observer.observe(node);
        return () => observer.disconnect();
    }, [measure, props.tabs.length]);

    /*
     * Das Rad, als eigener Griff mit `passive: false`.
     *
     * React haengt Rad-Ereignisse passiv ein, und ein passiver Griff darf die
     * Vorgabe nicht abbestellen. Ohne das Abbestellen wuerde in einer Leiste,
     * die nur waagerecht ueberlaeuft, zweimal gescrollt: einmal von Chromium,
     * das eine senkrechte Radbewegung dort selbst umlenkt, und einmal von hier.
     * Der Griff bleibt trotzdem, weil diese Umlenkung nicht in jeder Engine
     * dasselbe tut und die Leiste in allen scrollen soll.
     */
    useEffect(() => {
        const node = scroller.current;
        if (node === null) {
            return;
        }
        const onWheel = (event: WheelEvent): void => {
            const step = Math.abs(event.deltaX) > Math.abs(event.deltaY) ? event.deltaX : event.deltaY;
            if (step === 0 || node.scrollWidth <= node.clientWidth) {
                return;
            }
            event.preventDefault();
            node.scrollLeft += step;
            measure();
        };
        node.addEventListener('wheel', onWheel, { passive: false });
        return () => node.removeEventListener('wheel', onWheel);
    }, [measure]);

    // Den aktiven Tab holen, sobald er wechselt.
    useEffect(() => {
        const node = scroller.current;
        if (node === null || activePath.length === 0) {
            return;
        }
        // Verglichen wird der Wert und nicht ein zusammengebauter Selektor: ein
        // Dateipfad darf jedes Zeichen enthalten, und ein Selektor, der ihn
        // einbaut, ist eine Fundstelle, an der ein Anfuehrungszeichen im
        // Dateinamen die Suche kaputtmacht.
        const tab = [...node.querySelectorAll<HTMLElement>('.atlas-tab')]
            .find((candidate) => candidate.dataset.path === activePath);
        // jsdom kennt scrollIntoView nicht. Das ist kein Grund, ihm eine
        // Funktion unterzuschieben, die der Browser anders macht.
        if (typeof tab?.scrollIntoView === 'function') {
            tab.scrollIntoView({ block: 'nearest', inline: 'nearest' });
        }
        measure();
    }, [activePath, measure, props.tabs.length]);

    return (
        <div
            className="atlas-tabs-bar"
            data-testid="atlas-tabs-bar"
            data-overflowing={overflow.any}
            data-dragging={dragging}
        >
            <div
                className="atlas-tabs"
                data-testid="atlas-tabs"
                role="tablist"
                ref={scroller}
                onScroll={measure}
                /*
                  * Ein Zug ist kein Klick. Ohne das waehlte jeder Zug den Tab
                  * aus, ueber dem die Maus zufaellig losgelassen wurde.
                  */
                onClickCapture={(event) => {
                    if (dragged.current) {
                        event.preventDefault();
                        event.stopPropagation();
                        dragged.current = false;
                    }
                }}
                onPointerDown={(event) => {
                    const node = scroller.current;
                    if (node === null || event.button !== 0) {
                        return;
                    }
                    dragged.current = false;
                    drag.current = { startX: event.clientX, startScroll: node.scrollLeft, moved: 0 };
                }}
                onPointerMove={(event) => {
                    const node = scroller.current;
                    const state = drag.current;
                    if (node === null || state === null) {
                        return;
                    }
                    const delta = event.clientX - state.startX;
                    state.moved = Math.max(state.moved, Math.abs(delta));
                    if (state.moved < 4) {
                        return;
                    }
                    dragged.current = true;
                    setDragging(true);
                    node.scrollLeft = state.startScroll - delta;
                    measure();
                }}
                onPointerUp={() => {
                    drag.current = null;
                    setDragging(false);
                }}
                onPointerLeave={() => {
                    drag.current = null;
                    setDragging(false);
                }}
            >
                {props.tabs.length === 0 ? (
                    <span className="atlas-tabs-empty">{messages.app.noFileOpen}</span>
                ) : (
                    props.tabs.map((tab) => (
                        <span key={tab.path} className="atlas-tab" data-active={tab.active} data-path={tab.path}>
                            <Hint name="tab" text={tab.path}>
                                <button
                                    type="button"
                                    className="atlas-tab-label"
                                    role="tab"
                                    aria-selected={tab.active}
                                    data-testid="atlas-tab"
                                    onClick={() => props.onSelectTab(tab.path)}
                                >
                                    {tab.name}
                                </button>
                            </Hint>
                            {tab.active && <span className="atlas-tab-dot">●</span>}
                            <button
                                type="button"
                                className="atlas-tab-close"
                                aria-label={messages.app.closeTab(tab.name)}
                                onClick={() => props.onCloseTab(tab.path)}
                            >
                                x
                            </button>
                        </span>
                    ))
                )}
            </div>
            {(['left', 'right'] as const).map((side) => (
                <span
                    key={side}
                    className="atlas-tabs-overflow"
                    data-testid="atlas-tabs-overflow"
                    data-side={side}
                    data-on={side === 'left' ? overflow.left : overflow.right}
                    aria-hidden="true"
                />
            ))}
        </div>
    );
}

export const OPEN_COMMAND_SEARCH_EVENT = 'cbm:open-command-search';
export const CLOSE_COMMAND_SEARCH_EVENT = 'cbm:close-command-search';

export default function AtlasChrome(props: AtlasChromeProps): JSX.Element {
    /*
     * Ob der Fokus IRGENDWO in der Kommandozeile steht, das Feld oder eines
     * ihrer Beispiele.
     *
     * Nicht nur am Feld, und das ist die ganze Bedingung dafuer, dass die
     * Beispiele mit der Tastatur erreichbar sind: waeren sie an den Fokus des
     * FELDES gebunden, wuerde die Tabulatortaste sie in dem Moment schliessen,
     * in dem sie den ersten von ihnen erreicht. Ein Angebot, das verschwindet,
     * sobald man danach greift, ist keines.
     */
    /*
     * Ob die Kommandozeile den Fokus hat.
     *
     * Der Zustand gehoert hierher und nicht nach oben: er entsteht am Feld, er
     * faerbt das Feld, und niemand sonst braucht ihn. Ein `:focus-within` in CSS
     * taete dasselbe fuer das Auge, aber nicht fuer den Beweislauf, der lesen
     * koennen soll, was die Oberflaeche ueber sich behauptet.
     */
    const [commandFocused, setCommandFocused] = useState(false);
    const [chatWidth, setChatWidth] = useState(420);
    const [graphHeight, setGraphHeight] = useState(380);
    const [searchOpen, setSearchOpen] = useState(false);
    const searchDialog = useRef<HTMLDialogElement>(null);
    const searchButton = useRef<HTMLButtonElement>(null);
    const searchOpener = useRef<HTMLElement | null>(null);
    const inlineGalaxyHost = useRef<HTMLDivElement>(null);
    const chatGalaxyHost = useRef<HTMLDivElement>(null);
    const inlineInspectorHost = useRef<HTMLDivElement>(null);
    const chatInspectorHost = useRef<HTMLDivElement>(null);
    const hasInspector = props.selectionInspector !== undefined;
    const isExplore = (props.workspace ?? 'explore') === 'explore';
    const inspectorAboveChat = props.chatOpen === true && isExplore && hasInspector;
    // Moving the portal host preserves disclosures and focusable context controls.
    const [inspectorMount] = useState(() => {
        if (typeof document === 'undefined') return null;
        const element = document.createElement('div');
        element.className = 'atlas-selection-mount';
        return element;
    });
    useLayoutEffect(() => {
        const host = inspectorAboveChat ? chatInspectorHost.current : inlineInspectorHost.current;
        if (host !== null && inspectorMount !== null) host.appendChild(inspectorMount);
        return () => { inspectorMount?.remove(); };
    }, [hasInspector, inspectorAboveChat, inspectorMount]);
    // A stable portal keeps the WebGL canvas and its camera alive between hosts.
    const [galaxyMount] = useState(() => {
        if (typeof document === 'undefined') return null;
        const element = document.createElement('div');
        element.className = 'atlas-galaxy-mount';
        return element;
    });
    const graphBelowChat = props.chatOpen === true &&
        (props.workspace ?? 'explore') === 'explore' && props.galaxy !== undefined;
    useLayoutEffect(() => {
        const host = graphBelowChat ? chatGalaxyHost.current : inlineGalaxyHost.current;
        if (host !== null && galaxyMount !== null) host.appendChild(galaxyMount);
        return () => { galaxyMount?.remove(); };
    }, [galaxyMount, graphBelowChat]);

    const openSearch = useCallback(() => {
        if (!searchDialog.current?.open) {
            searchOpener.current = document.activeElement instanceof HTMLElement
                ? document.activeElement : null;
        }
        setSearchOpen(true);
        if (searchDialog.current?.open) searchDialog.current.querySelector<HTMLInputElement>('.atlas-command-input')?.focus();
    }, []);
    const closeSearch = useCallback(() => setSearchOpen(false), []);
    useEffect(() => {
        const onKey = (event: globalThis.KeyboardEvent): void => {
            if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === 'k') {
                event.preventDefault();
                event.stopPropagation();
                openSearch();
            }
        };
        window.addEventListener(OPEN_COMMAND_SEARCH_EVENT, openSearch);
        window.addEventListener(CLOSE_COMMAND_SEARCH_EVENT, closeSearch);
        window.addEventListener('keydown', onKey, true);
        return () => {
            window.removeEventListener(OPEN_COMMAND_SEARCH_EVENT, openSearch);
            window.removeEventListener(CLOSE_COMMAND_SEARCH_EVENT, closeSearch);
            window.removeEventListener('keydown', onKey, true);
        };
    }, [openSearch, closeSearch]);
    useLayoutEffect(() => {
        const dialog = searchDialog.current;
        if (dialog === null) return;
        if (searchOpen) {
            if (!dialog.open) {
                if (typeof dialog.showModal === 'function') dialog.showModal();
                else dialog.setAttribute('open', '');
            }
            dialog.querySelector<HTMLInputElement>('.atlas-command-input')?.focus();
        } else if (dialog.open) {
            if (typeof dialog.close === 'function') dialog.close();
            else dialog.removeAttribute('open');
            const opener = searchOpener.current;
            if (opener?.isConnected && !dialog.contains(opener)) opener.focus();
            else searchButton.current?.focus();
        }
    }, [searchOpen]);
    const examples = props.commandExamples ?? [];
    const showExamples =
        commandFocused && examples.length > 0 && props.commandValue.trim().length === 0;

    /*
     * Die Masse mit ihrer Vorgabe, an einer Stelle.
     *
     * Optional, weil das Chrome in jsdom auch ohne Layout-Zustand gezeichnet
     * werden koennen soll (src/app/AtlasChrome.test.tsx zeichnet es ohne). Ohne
     * Angabe gilt die Vorgabe aus dem Modell und nicht eine zweite Zahl hier:
     * eine Vorgabe, die es zweimal gibt, ist keine.
     */
    const zones = props.zones ?? LAYOUT_DEFAULT;
    const hasSide =
        hasInspector || props.twin !== undefined || props.galaxy !== undefined || props.llm !== undefined;

    const stopTabKeys = (event: KeyboardEvent<HTMLInputElement>): void => {
        if (event.key === 'Escape') {
            event.preventDefault();
            event.stopPropagation();
            closeSearch();
            return;
        }
        props.onCommandKeyDown?.(event);
    };

    return (
        <div className="atlas-shell" data-workspace={props.workspace ?? 'explore'} data-guidance={props.guidance ?? 'brief'} data-chat-open={props.chatOpen === true} data-has-status={props.workspaceStatus !== undefined}>
            <header className="atlas-header" data-testid="atlas-header">
                <h1 className="atlas-brand">{messages.app.brand}</h1>
                <span className="atlas-version" data-testid="atlas-version">
                    {props.version}
                </span>
                {(props.buildSuffix ?? '').length > 0 && (
                    <span className="atlas-version-suffix" data-testid="atlas-version-suffix">
                        {props.buildSuffix}
                    </span>
                )}
                {/*
                  * Sechs Eintraege, EINE Gestalt.
                  *
                  * Bis W7b gab es zwei: `atlas-menu-item` fuer die beiden
                  * Punkte mit eigenem Buchstaben in der Zeile und
                  * `atlas-menu-extra` fuer die vier Eintraege der Atlas-Zeile,
                  * kleiner gesetzt und mit einem gepunkteten Unterstrich statt
                  * eines Rahmens. Der Nutzer hat am 2026-08-29 genau das
                  * gemeldet: die vier sahen aus wie Text. Sie sind es nicht,
                  * also sehen sie jetzt auch nicht so aus. Eine Klasse, ein
                  * Rahmen, eine Polsterung, ein Hover, ein Fokusring, und der
                  * Klammer-Buchstabe an derselben Stelle in Phosphor.
                  */}
                {props.onWorkspaceChange !== undefined && (
                    <nav className="atlas-workspace-tabs" role="tablist" aria-label={workspaceStrings.navigation}>
                        {workspaceStrings.workspaces.map((workspace, index) => (
                            <button type="button" role="tab" key={workspace.id} data-workspace-tab={workspace.id}
                                tabIndex={(props.workspace ?? 'explore') === workspace.id ? 0 : -1}
                                aria-selected={(props.workspace ?? 'explore') === workspace.id}
                                onKeyDown={(event) => {
                                    const choices = workspaceStrings.workspaces;
                                    const next = event.key === 'ArrowRight' ? (index + 1) % choices.length
                                        : event.key === 'ArrowLeft' ? (index + choices.length - 1) % choices.length
                                        : event.key === 'Home' ? 0 : event.key === 'End' ? choices.length - 1 : -1;
                                    if (next < 0) return;
                                    event.preventDefault();
                                    props.onWorkspaceChange?.(choices[next].id);
                                    (event.currentTarget.parentElement?.querySelectorAll('button')[next] as HTMLButtonElement | undefined)?.focus();
                                }}
                                onClick={() => props.onWorkspaceChange?.(workspace.id)}>
                                {workspace.label}
                            </button>
                        ))}
                    </nav>
                )}
                {props.onOpenSystem !== undefined && <button type="button" className="atlas-daemon-action" data-state={props.daemonState ?? 'checking'} onClick={props.onOpenSystem}
                    aria-label={workspaceStrings.daemonNavigation(props.daemonState ?? 'checking')}><span aria-hidden="true" />{workspaceStrings.daemon}</button>}
                {props.onOpenBrowserAi !== undefined && <button type="button" className="atlas-browser-ai-action" aria-expanded={props.chatOpen === true} onClick={props.onOpenBrowserAi}>{workspaceStrings.browserAi}</button>}
                <button ref={searchButton} type="button" className="atlas-search-action"
                    aria-label={workspaceStrings.searchOpen} aria-haspopup="dialog"
                    aria-expanded={searchOpen} onClick={openSearch}>
                    {workspaceStrings.search}<kbd>{workspaceStrings.searchShortcut}</kbd>
                </button>
                <details className="atlas-tools-menu" onClick={(event) => {
                    if ((event.target as HTMLElement).closest('button')) event.currentTarget.open = false;
                }} onKeyDown={(event) => {
                    if (event.key === 'Escape') { event.currentTarget.open = false; event.currentTarget.querySelector('summary')?.focus(); }
                }}>
                <summary>{workspaceStrings.tools}</summary>
                <nav className="atlas-menu" data-testid="atlas-menu" aria-label={messages.menu.ariaLabel}>
                    {MENU_ITEMS.map((item) => {
                        const wiring = props.menus?.[item.key];
                        return wiring === undefined ? null : (
                            <span className="atlas-menu-group" key={item.key}>
                                <Hint name={`menu-${item.key}`} text={wiring.title}>
                                    <button
                                        type="button"
                                        className="atlas-menu-item"
                                        data-state={wiring.state}
                                        data-menu={item.key}
                                        data-testid="atlas-menu-item"
                                        aria-pressed={wiring.state === 'on'}
                                        onClick={wiring.onSelect}
                                    >
                                        <span className="atlas-menu-key">[{item.key}]</span>
                                        {item.rest}
                                    </button>
                                </Hint>
                                {(wiring.extras ?? [])
                                    .filter((extra) => extra.key !== 'bug' && extra.key !== 'impact')
                                    .map((extra) => {
                                    const parts = splitMenuLabel(extra.label);
                                    return (
                                        <Hint
                                            key={extra.key}
                                            name={`menu-${item.key}-${extra.key}`}
                                            text={extra.title}
                                        >
                                            <button
                                                type="button"
                                                className="atlas-menu-item"
                                                /*
                                                 * `off` und nicht ein dritter
                                                 * Wert: ein Eintrag der
                                                 * Atlas-Zeile schaltet eine
                                                 * Flaeche auf, die beim Zeichnen
                                                 * der Zeile noch zu ist. `on`
                                                 * waere die Behauptung, sie
                                                 * stuende schon da.
                                                 */
                                                data-state="off"
                                                data-menu={`${item.key}-${extra.key}`}
                                                data-testid="atlas-menu-item"
                                                onClick={extra.onSelect}
                                            >
                                                {parts.key.length > 0 && (
                                                    <span className="atlas-menu-key">[{parts.key}]</span>
                                                )}
                                                {parts.rest}
                                            </button>
                                        </Hint>
                                    );
                                })}
                            </span>
                        );
                    })}
                    {/*
                      * Was die Klammer bedeutet, in der Zeile selbst.
                      *
                      * Seit dem 2026-08-29 gilt ein Menuebuchstabe nur mit
                      * Alt/Option (src/app/keyboard.ts nennt den Grund: blanke
                      * Buchstaben gehoeren dem Tippen). Ein `[a]` ohne diese
                      * Angabe waere genau die Sorte Versprechen, die dieser
                      * Zyklus aus der Zeile entfernt hat.
                      */}
                    <span className="atlas-menu-legend" data-testid="atlas-menu-legend">
                        {messages.menu.legend}
                    </span>
                </nav>
                </details>
                {props.onGuidanceChange !== undefined && (
                    <select className="atlas-guidance" aria-label={workspaceStrings.guidance}
                        value={props.guidance ?? 'brief'}
                        onChange={(event) => props.onGuidanceChange?.(event.target.value as Guidance)}>
                        <option value={workspaceStrings.briefValue}>{workspaceStrings.brief}</option>
                        <option value={workspaceStrings.explainedValue}>{workspaceStrings.explained}</option>
                    </select>
                )}
                <div className="atlas-chips">
                    {props.chips.filter(chip => props.projectSwitcher === undefined || chip.label !== messages.statusbar.chipProject).map((chip) => (
                        <ChipView key={chip.label} chip={chip} />
                    ))}
                </div>
                {props.projectSwitcher}
            </header>

            {props.workspaceStatus}
            <div className="atlas-workspace-content" style={{ '--atlas-chat-width': `${chatWidth}px` } as CSSProperties}>
            <div className="atlas-workspace-stage">
            <div className="atlas-exploration-workspace" data-testid="atlas-exploration-workspace" hidden={props.workspace !== undefined && props.workspace !== 'explore' && props.workspace !== 'galaxy'}>
            <TabBar tabs={props.tabs} onSelectTab={props.onSelectTab} onCloseTab={props.onCloseTab} />

            {/*
              * `data-twin` heisst seit W3 "es gibt eine rechte Spalte" und
              * nicht mehr "es gibt einen Twin": in ihr stehen jetzt zwei
              * Flaechen. Der Name bleibt, weil er in der CSS-Regel und in den
              * bisherigen Beweisbildern steht, und ein umbenanntes Attribut
              * waere eine Aenderung an allem davon fuer nichts.
              *
              * Seit W8 traegt der Rumpf ausserdem die drei Masse als
              * CSS-Variablen. Sie stehen hier und nicht an den Zonen, weil sie
              * das RASTER beschreiben und nicht die Kaesten darin: eine Spalte
              * kennt ihre Breite nicht, ein Raster schon.
              */}
            <div
                className="atlas-body"
                data-testid="atlas-body"
                data-twin={hasSide}
                style={{
                    '--atlas-left-w': `${zones.leftWidth}px`,
                    '--atlas-right-w': `${hasInspector ? Math.max(360, zones.rightWidth) : zones.rightWidth}px`,
                    '--atlas-twin-h': `${zones.twinHeight}px`,
                } as CSSProperties}
            >
                <AtlasTree {...props.tree} />
                {props.splitLeft}
                <main className="atlas-main" data-testid="atlas-main">
                    <div className="atlas-breadcrumb" data-testid="atlas-breadcrumb">
                        {props.breadcrumb.length === 0 ? (
                            <span className="atlas-breadcrumb-empty">{messages.app.noFileOpen}</span>
                        ) : (
                            props.breadcrumb.map((segment, index) => (
                                <span key={`${segment}-${index}`}>
                                    {index > 0 && ' › '}
                                    <span
                                        className={
                                            index === props.breadcrumb.length - 1
                                                ? 'atlas-breadcrumb-leaf'
                                                : 'atlas-breadcrumb-step'
                                        }
                                    >
                                        {segment}
                                    </span>
                                </span>
                            ))
                        )}
                        {props.readerActions !== undefined && <div className="atlas-reader-actions">{props.readerActions}</div>}
                    </div>
                    {props.coverageNote !== undefined && (
                        <p
                            className="atlas-coverage-note"
                            data-testid="atlas-coverage-note"
                            data-coverage={props.coverageNote.state}
                        >
                            <b>{props.coverageNote.state}: </b>
                            {props.coverageNote.text}
                        </p>
                    )}
                    <div className="atlas-reader" data-testid="atlas-reader">
                        {props.children}
                    </div>
                    {props.truncationNote.length > 0 && (
                        <p className="atlas-truncation" data-testid="atlas-truncation">
                            <b>{messages.reader.incompleteLabel}</b>
                            {props.truncationNote}
                        </p>
                    )}
                    {props.splitExplain}
                    {props.explain}
                </main>
                {hasSide && (
                    <>
                        {props.splitRight}
                        <div className="atlas-side" data-galaxy={props.galaxy !== undefined} data-selection-inspector={hasInspector}>
                            <div ref={inlineInspectorHost} className="atlas-selection-host" hidden={!isExplore || !hasInspector || inspectorAboveChat}>
                                {inspectorMount === null && props.selectionInspector}
                            </div>
                            {!hasInspector && props.llm}
                            {!hasInspector && props.twin}
                            {!hasInspector && props.splitTwin}
                            <div ref={inlineGalaxyHost} className="atlas-galaxy-host">{galaxyMount === null && props.galaxy}</div>
                        </div>
                    </>
                )}
            </div>

            </div>
            <main className="atlas-alternate-workspace" hidden={props.workspace === undefined || props.workspace === 'explore' || props.workspace === 'galaxy'}>{props.workspacePanel}</main>
            </div>
            {props.chatOpen === true && <Splitter testId="atlas-split-chat" orientation="vertical" label={workspaceStrings.chatWidth} value={chatWidth}
                min={300} max={600} invert onChange={setChatWidth} onReset={() => setChatWidth(420)} />}
            <div className="atlas-chat-column" hidden={props.chatOpen !== true} data-graph={graphBelowChat} data-selection-inspector={inspectorAboveChat}
                style={{ '--atlas-chat-graph-height': `${graphHeight}px` } as CSSProperties}>
                <div ref={chatInspectorHost} className="atlas-selection-host" hidden={!inspectorAboveChat} />
                {props.chatDock}
                {graphBelowChat && <Splitter testId="atlas-split-chat-graph" orientation="horizontal"
                    label={workspaceStrings.chatGraphHeight} value={graphHeight} min={320} max={640} invert
                    onChange={setGraphHeight} onReset={() => setGraphHeight(380)} />}
                <div ref={chatGalaxyHost} className="atlas-galaxy-host" hidden={!graphBelowChat} />
            </div>
            {galaxyMount !== null && createPortal(props.galaxy, galaxyMount)}
            {inspectorMount !== null && createPortal(props.selectionInspector, inspectorMount)}
            </div>

            {/*
              * Die Zeile sagt, ob sie den Fokus hat.
              *
              * Nutzerbefund vom 2026-08-29: ohne Fokus kam nichts an, und der
              * Zustand war der Zeile nicht anzusehen. Der Fokus faerbt jetzt den
              * Prompt und setzt einen Rahmen (CSS ueber `data-focused`), und der
              * Zustand steht im DOM, damit der Beweislauf ihn lesen kann statt
              * ihn aus Farben zu raten.
              *
              * Kein `title` mehr am Feld: der native Tooltip legte sich beim
              * Zeigen ueber den Anfang der Zeile. Was er sagte, steht in der
              * Hilfe ([?]help).
              */}
            <dialog ref={searchDialog} className="atlas-command-palette" aria-labelledby="atlas-search-title"
                onCancel={(event) => { event.preventDefault(); closeSearch(); }}
                onClose={closeSearch}
                onClick={(event) => {
                    if (event.target !== event.currentTarget) return;
                    const bounds = event.currentTarget.getBoundingClientRect();
                    if (event.clientX < bounds.left || event.clientX > bounds.right ||
                        event.clientY < bounds.top || event.clientY > bounds.bottom) closeSearch();
                }}>
            <header className="atlas-search-heading">
                <h2 id="atlas-search-title">{workspaceStrings.searchTitle}</h2>
                <button type="button" onClick={closeSearch} aria-label={workspaceStrings.searchClose}>×</button>
            </header>
            <div
                className="atlas-command"
                data-testid="atlas-command"
                data-hint-keep="command line"
                data-focused={commandFocused}
                data-examples={showExamples}
                onFocus={() => setCommandFocused(true)}
                onBlur={(event) => {
                    if (!event.currentTarget.contains(event.relatedTarget)) {
                        setCommandFocused(false);
                    }
                }}
            >
                <div className="atlas-search-options">{props.commandOverlay}</div>
                {showExamples && (
                    <div
                        className="atlas-command-examples"
                        data-testid="atlas-command-examples"
                        role="group"
                        aria-label={messages.command.examplesLabel}
                    >
                        <span className="atlas-command-examples-label">
                            {workspaceStrings.searchExamples}
                        </span>
                        {examples.map((example) => (
                            <button
                                key={example.id}
                                type="button"
                                className="atlas-command-example"
                                data-testid="atlas-command-example"
                                data-example={example.id}
                                data-symbol={example.symbol}
                                onMouseDown={(event) => {
                                    // Vor dem Blur des Feldes: ein Klick, der
                                    // erst nach dem Fokusverlust wirkt, traefe
                                    // eine Liste, die dann schon zu waere.
                                    event.preventDefault();
                                    props.onCommandExample?.(example.text);
                                }}
                                onClick={() => props.onCommandExample?.(example.text)}
                            >
                                <span className="atlas-command-example-text">{example.text}</span>
                                <span className="atlas-command-example-note">{example.note}</span>
                            </button>
                        ))}
                    </div>
                )}
                <span className="atlas-command-prompt" data-testid="atlas-command-prompt">
                    {'>'}
                </span>
                <input
                    ref={props.commandInputRef}
                    className="atlas-command-input"
                    data-testid="atlas-command-input"
                    aria-label={messages.command.ariaLabel}
                    placeholder={props.commandPlaceholder ?? COMMAND_PLACEHOLDER}
                    value={props.commandValue}
                    onChange={(event) => props.onCommandChange(event.target.value)}
                    onKeyDown={stopTabKeys}
                    autoComplete="off"
                />
                <span className="atlas-command-hint">{props.commandHint}</span>
            </div>

            </dialog>

            <div className="atlas-statusbar" data-testid="atlas-statusbar">
                {props.status.filter((chip) => chip.label === messages.statusbar.chipServer).map((chip) => (
                    <ChipView key={chip.label} chip={chip} />
                ))}
                <details className="atlas-status-details" open={props.guidance === 'explained'}>
                    <summary>{workspaceStrings.diagnostics}</summary>
                    <div>{props.status.filter((chip) => chip.label !== messages.statusbar.chipServer).map((chip) => (
                        <ChipView key={chip.label} chip={chip} />
                    ))}</div>
                </details>
            </div>
            {props.globalOverlay}
        </div>
    );
}
