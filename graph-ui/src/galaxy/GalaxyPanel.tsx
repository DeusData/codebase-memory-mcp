/**
 * Die Galaxie als Panel, und der Fokus, der in beide Richtungen laeuft.
 *
 * Die Szene selbst ist uebernommen (src/galaxy/GraphScene.tsx, MIT, DeusData);
 * diese Datei ist das, was dieses Projekt daraus macht. Sie haelt fuenf
 * Entscheidungen, und jede davon ist eine Antwort auf eine Frage, die man beim
 * Lesen sofort stellt:
 *
 * 1. **Das Panel laedt selbst.** Nicht die App: `/api/layout` ist die einzige
 *    Route, die dieses Panel braucht, und niemand sonst im Haus braucht sie.
 *    Ein Fehler wird gezeigt, nicht verschluckt (src/galaxy/layout-source.ts).
 *    Was geladen wurde, meldet das Panel nach oben (`onLayout`), weil die
 *    Bedeutungssuche daraus ihre Fan-in-Zahlen nimmt und der Server sie in der
 *    flachen Suchform nicht mitschickt (UPSTREAM-ASKS.md, Ask 5).
 * 2. **Einmal sichtbar heisst gemountet.** Wird das Panel zugeklappt, bleibt
 *    die Szene im Baum und nur der Renderloop steht still (`active`). Ein
 *    Aushaengen wuerde den WebGL-Kontext wegwerfen, und der naechste Aufklapper
 *    zahlte dafuer mit Sekunden und einem schwarzen Kasten.
 * 3. **Die Kamera faehrt bei neuer Objekt-Identitaet.** So ist der
 *    CameraAnimator der Uebernahme gebaut: `useEffect([target])`. Also wird
 *    fuer jede Fahrt ein frisches Ziel gerechnet, auch wenn es zweimal
 *    dasselbe Symbol ist, und der Zaehler `targetChanges` im Testgriff zaehlt
 *    genau diese frischen Ziele.
 * 4. **Ein Symbol ohne Knoten sagt das.** Der Deckel liegt bei 5000 Knoten;
 *    was darueber liegt, ist nicht im Bild. Ein stiller No-op waere die
 *    Behauptung, das Symbol sei gezeigt worden.
 * 5. **Unter dem Kopf steht, was man sieht.** Die Legende (W4d, Nutzerfeedback)
 *    erklaert Knotenfarbe, Knotengroesse, Kantenfarben, Fokus und Positionen,
 *    und zwar aus den Quellen, die sie erzeugen: src/galaxy/galaxy-legend.ts.
 *    Sie ist auf- und zuklappbar, weil dieses Panel 420 Pixel hoch ist und ein
 *    Leser, der die Erklaerung gelesen hat, den Platz wiederhaben will; der
 *    Zustand liegt im localStorage, damit die Entscheidung den Reload
 *    ueberlebt.
 *
 * Seit W4e zeigt dasselbe Panel zwei Bilder, und daraus kommen vier weitere
 * Entscheidungen:
 *
 * 6. **Ein Canvas, zwei Datensaetze.** Der Chip-Schalter tauscht die Daten der
 *    Szene und nicht die Szene. Ein zweites `<Canvas>` waere ein zweiter
 *    WebGL-Kontext, und ein bedingtes Aushaengen waere derselbe schwarze
 *    Kasten wie in Entscheidung 2, nur bei jedem Umschalten.
 * 7. **Die Hierarchie ist die Vorgabe, sobald es einen Walk gibt.** Wer sich
 *    fuer einen Einstiegspunkt entschieden hat, hat eine Frage nach der Tiefe
 *    gestellt, und die Wolke beantwortet sie nicht. Ein Klick auf `galaxy`
 *    holt das alte Bild zurueck, und diese Wahl haelt die Sitzung: sie ist
 *    eine Antwort und keine Geste. Ohne Walk gibt es nichts zu projizieren,
 *    also steht der Schalter dann auf `galaxy` und der andere Chip sagt, was
 *    ihm fehlt.
 * 8. **Die Kamera rahmt den ganzen Subgraphen, nicht den Schritt.** Genau
 *    darum geht es in dieser Ansicht: man soll die Tiefe SEHEN. Eine Kamera,
 *    die bei jedem Schritt auf eine Spalte zoomt, zeigt wieder nur die
 *    Nachbarschaft, also faehrt sie einmal je Projektion und danach bewegt
 *    sich nur noch der Ring.
 * 9. **Der Ring ist DOM und keine Geometrie.** Der Schritt, auf dem der Leser
 *    steht, bekommt einen pulsenden Ring ueber seinem Punkt (`Html` aus drei
 *    der Uebernahme, dieselbe Technik wie die Hover-Karte). Ihn aus Dreiecken
 *    zu bauen hiesse, bei jedem Schritt die Puffer der Szene neu zu bauen und
 *    fuer die Animation bei jedem Bild.
 *
 * Seit W9 ist die Legende auch der Filter, und daraus kommen zwei weitere:
 *
 * 10. **Gefiltert wird zwischen Bild und Szene, nicht in der Szene.** Das Panel
 *     rechnet aus der geladenen Antwort zuerst das BILD (in der Hierarchie samt
 *     der Beziehungen, die der Index ausser den Aufrufen kennt) und reicht der
 *     Szene davon nur die Arten weiter, die sichtbar sein sollen. Die Szene
 *     kennt keinen Filter, die Legende zaehlt am Bild und nicht am Gefilterten,
 *     und beide Ansichten teilen sich denselben Satz ausgeblendeter Arten:
 *     wer in der Galaxie die Importe weggenommen hat, findet sie in der
 *     Hierarchie nicht wieder.
 * 11. **Der Nachbarschafts-Fokus rechnet weiter am ganzen Bild.** Ein Klick
 *     hebt die Nachbarn eines Symbols hervor, und Nachbar ist, wen der Index
 *     nennt, nicht wen der Filter gerade durchlaesst. Sonst waere dieselbe
 *     Frage je nach Filterlage anders beantwortet, ohne dass die Antwort das
 *     sagt.
 * 12. **Die Legende zeigt ihre Kante.** Der Kasten ist niedriger als sein
 *     Inhalt, und der Bildlauf dieser Plattform ist eine ueberlagernde Leiste,
 *     die im Ruhezustand nicht zu sehen ist. Ohne einen eigenen Hinweis endet
 *     der letzte sichtbare Satz darum an einer harten Kante mitten im Wort, und
 *     das liest sich als Fehler und nicht als Fortsetzung. Das Panel misst
 *     deshalb selbst, ob ueber oder unter dem Kasten noch etwas steht, und sagt
 *     es: ein Verlauf loest die Kante auf, eine Marke nennt die Richtung.
 *
 * Seit W11a liegt eine dritte Ebene ueber demselben Canvas, und daraus kommen
 * drei weitere Entscheidungen:
 *
 * 13. **Die Agentenebene faerbt nichts um.** Sie haengt als `overlay` in
 *     derselben Szene und legt eigene Koerper ueber die Punkte; kein Knoten und
 *     keine Kante aendert dabei ihre Farbe. Warum das die einzige vertretbare
 *     Form ist, steht im Kopf von src/galaxy/AgentLayer.tsx.
 * 14. **Verortet wird HIER, weil hier das Layout liegt.** Ein Ereignis nennt
 *     einen Pfad und manchmal Zeilen; welcher Knoten das ist, weiss nur, wer die
 *     Layout-Antwort hat. Die Rechnung steht in src/agents/agent-view.ts und
 *     wird EINMAL gemacht: die Ebene zeichnet aus demselben Ergebnis, aus dem
 *     das Instrument seine Zeilen schreibt.
 * 15. **Der Leser ist ein Akteur wie die anderen.** Oeffnet er ein Symbol,
 *     entsteht hier ein Ereignis mit dem Namen "you", das durch dieselbe Ebene
 *     laeuft. Es geht NICHT in die Ereignisdatei: die Bruecke hat keine Route,
 *     die etwas entgegennimmt, und dieses Ereignis verlaesst das Fenster nie.
 *
 * Seit W10b kommen drei Entscheidungen aus vier Nutzerbefunden dazu:
 *
 * 16. **Die beiden Ansichts-Knoepfe klappen auch.** Ein Klick auf den AKTIVEN
 *     Knopf klappt die Sektion zu, ein Klick bei zugeklappter Sektion klappt sie
 *     auf und waehlt diese Ansicht. Das ist die Antwort auf den Befund "galaxy
 *     Knopf macht nichts" (2026-08-29) und auf den Auftrag vom Folgetag: "die
 *     beiden Buttons unten links sollten auch aufklappen und zuklappen koennen."
 *     Der beschriftete Ein- und Ausklapper daneben bleibt, und beide Wege gehen
 *     durch DENSELBEN Rueckruf: zwei Wege in denselben Zustand duerfen nicht
 *     zwei Zustaende ergeben.
 * 17. **Die Hierarchie waechst auch aus dem Fokus, waehlt sich aber nicht
 *     selbst.** Bis W10b gab es sie nur nach einem Einstiegs-Spaziergang, und
 *     ein Leser mit einem Symbol vor sich sah einen grauen Knopf (Befund
 *     2026-08-30). Der Walk aus dem Fokus kommt von aussen (`focusWalk`), weil
 *     nur die App einen Provider hat; er hat denselben Vorwaerts-Closure und
 *     dieselben Grenzen. Die VORGABE bleibt trotzdem die Galaxie: ein Fokus
 *     entsteht bei jedem Klick in den Code, und eine Ansicht, die dabei von
 *     selbst umschaltet, waere ein Bild, das der Leser nie bestellt hat. Der
 *     Kopf sagt, woher die Wurzel kommt.
 * 18. **Die Kamera passt das Bild ein, sofort und nicht im Anflug.** Beim
 *     Oeffnen, beim Aufklappen, bei einer neuen Groesse der Zeichenflaeche und
 *     auf Knopfdruck steht sie senkrecht auf der groessten Flaeche der Wolke,
 *     weit genug weg, dass JEDER Knoten im Bild liegt (src/galaxy/camera-frame.ts).
 *     Ohne Anflug, weil eine Einpassung keine Bewegung ist, sondern die Lage, in
 *     der das Bild anfaengt. In der Hierarchie bleibt es bei der frontalen
 *     Rahmung aus W5c: sie ist eine flache Zeichnung mit Spalten, und eine
 *     Kamera, die deren Hauptachsen folgt, stellte das Raster schief.
 */

import type { JSX, ReactNode } from 'react';
import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { Html } from '@react-three/drei';

import { GraphScene, computeCameraTarget, computeFitTarget, computeFrameTarget } from './GraphScene';
import type { CameraTarget } from './GraphScene';
import type { LabelBox } from './NodeLabels';
import { NodeTooltipCard } from './NodeTooltipCard';
import {
    GALAXY_NO_FOCUS_NOTE,
    LAYOUT_NODE_BUDGET,
    layoutSummary,
    missingNodeNote,
    neighbourIds,
    nodesByQualifiedName,
    unopenableNodeNote,
} from './galaxy-model';
import { loadLayout } from './layout-source';
import { DEFAULT_DISPLAY_SETTINGS, DEFAULT_GRAPH_DISPLAY, displayWith } from './density';
import type { DisplaySettings, GraphDisplaySettings } from './density';
import {
    edgeKindNote,
    edgeKinds,
    galaxyLegendEntries,
    hierarchyLegendEntries,
    readLegendOpen,
    withoutEdgeKinds,
    writeLegendOpen,
} from './galaxy-legend';
import type { EdgeKind } from './galaxy-legend';
import {
    HIERARCHY_LABEL_BUDGET,
    HIERARCHY_LABEL_FONT_SIZE,
    HIERARCHY_LABEL_MAX_TEXT_WIDTH,
    hierarchyEdgeNote,
    hierarchyFrame,
    hierarchyHeadline,
    hierarchyIndexEdges,
    projectHierarchy,
} from './hierarchy-layout';
import type { HierarchyRootOrigin } from './hierarchy-layout';
import type { ClosureResult } from '../provider/closure';
import Hint from '../ui/tooltip/Hint';
import type { GraphData, GraphNode } from './types';
import {
    AgentLayer,
    agentAngles,
    agentCamera,
    agentMotion,
    agentPositions,
    agentPulseScale,
    agentRenderOrders,
    agentTails,
    orbitRadii,
} from './AgentLayer';
import AgentsHud from '../agents/AgentsHud';
import type { HudSwitches } from '../agents/AgentsHud';
import AgentsTimeline from '../agents/AgentsTimeline';
import { YOU_ID } from '../agents/agent-colors';
import { workKindOf } from '../agents/agent-event';
import { buildPlacementIndex } from '../agents/agent-placement';
import { buildAgentsView } from '../agents/agent-view';
import type { ActorFilter, ActorView, HudSize } from '../agents/agent-view';
import { stateUntil } from '../agents/agent-store';
import { buildTimeline, TIMELINE_MIN_WIDTH } from '../agents/agent-timeline';
import { DRAWN_BODIES_CAP, TRANSITION_MS } from '../agents/agent-motion';
import { WORK_KIND_WORD, agentStrings as agentText } from '../agents/agent-strings';
import type { AgentsRuntime } from '../agents/agent-source';
import { loadAgentsPreference, saveAgentsPreference } from '../agents/agent-preference';
import type { AgentsPreference } from '../agents/agent-preference';
import { fullscreenIsolationRequired, isolateFullscreenBackground } from './fullscreen-isolation';

/**
 * Wie lange die Ereigniszeile des FOLLOW-Modus stehen bleibt.
 *
 * Lang genug, um sie zu lesen, kurz genug, dass sie nicht zur Beschriftung des
 * Bildes wird: sie gehoert zu einem Anflug, der vorbei ist.
 */
export const FOLLOW_LINE_MS = 4000;

/** Welches der beiden Bilder das Panel gerade zeigt. */
export type GraphMode = 'galaxy' | 'hierarchy';

/** Die beiden Chips, in der Reihenfolge, in der sie im Kopf stehen. */
export const GRAPH_MODES: readonly GraphMode[] = ['galaxy', 'hierarchy'];

/**
 * Was der `hierarchy`-Chip sagt, solange es weder Walk noch Fokus gibt.
 *
 * Seit W10b nennt er BEIDE Wege, weil es seitdem beide gibt (AC3): ein Symbol
 * im Fokus genuegt, ein Einstiegs-Spaziergang ist der andere Weg. Ein Knopf, der
 * nur den umstaendlicheren nennt, schickt den Leser auf den Umweg, den dieser
 * Zyklus gerade abgeschafft hat.
 */
export const HIERARCHY_UNAVAILABLE_TITLE =
    'hierarchy: open a symbol or pick a way in first, then this shows what it reaches';

/** Was das Panel in der Hierarchie sagt, wenn der Leser ausserhalb des Walks steht. */
export const HIERARCHY_NO_FOCUS_NOTE =
    'nothing of this walk is in focus: the ring follows the symbol in front of the reader';

/**
 * Wie die Szene im hierarchy-Modus eingestellt ist.
 *
 * Bloom aus und der Knotenglanz halbiert, und das ist die Antwort auf einen
 * Befund und keine Vorliebe (Nutzerfeedback 2026-08-29, Screenshots): das
 * Leuchten ist die Sprache der Galaxie, wo tausende Punkte eine Wolke bilden
 * und der Glanz die Dichte lesbar macht. Hier stehen hoechstens sechzig Punkte
 * in einem Raster, und dieselbe Nachbearbeitung macht aus jedem Namen einen
 * Fleck. Die Antwort dieser Ansicht ist Struktur und nicht Glanz.
 */
export const HIERARCHY_DISPLAY: DisplaySettings = {
    ...DEFAULT_DISPLAY_SETTINGS,
    bloom: 0,
    nodeGlow: 0.5,
};

/** Der Schalter im Panel-Kopf, in beiden Lagen. */
export const GALAXY_COLLAPSE_TITLE = 'hide the graph panel: the header stays, so it can be brought back';
export const GALAXY_EXPAND_TITLE = 'show the graph panel again';

/**
 * Wie der Zuklapp-Schalter heisst, und warum er ein Wort traegt statt eines
 * Zeichens.
 *
 * Bis W8b stand dort `[-]` beziehungsweise `[+]`, und was sie bedeuten, stand
 * nur im Tooltip. Der Nutzer hat es beim Testen am 2026-08-29 nicht verstanden
 * und woertlich gefordert: "wenn Galaxy aufgeklappt ist, sollte 'einklappen
 * Galaxy' auf Englisch stehen, und wenn Galaxy geschlossen ist 'open Galaxy',
 * analog zu hierarchy." Das ist die Auskunft, die zaehlt.
 *
 * Der Name folgt dabei dem, was gerade ZU SEHEN ist, und nicht dem Panel:
 * dasselbe Panel zeigt zwei Bilder, und "collapse galaxy" ueber einer
 * Hierarchie waere ein Schalter, der ein anderes Bild verspricht als das, das
 * er zumacht.
 */
export function graphFoldLabel(open: boolean, mode: GraphMode): string {
    return `${open ? 'collapse' : 'open'} ${mode}`;
}

/**
 * Was der Schalter der Legende sagt. Dieselbe Regel, kuerzere Woerter.
 *
 * "hide" und "show" statt "collapse" und "open", und das ist genau der Weg, den
 * AC3 fuer diesen Fall nennt: "Passt das Wort in einer engen Spalte nicht, wird
 * die Spalte breiter oder das Wort kuerzer, nicht das Zeichen wieder
 * eingesetzt." Vier Zeichen weniger sind hier der Unterschied zwischen einer
 * Kopfzeile mit einer Zeile und einer mit zwei, und zwei Zeilen ueber einem
 * Canvas kosten die Szene ihre Hoehe. Der Zustand steht weiter im Wort, und das
 * ist die Zusicherung, um die es geht.
 */
export function legendFoldLabel(open: boolean): string {
    return `${open ? 'hide' : 'show'} legend`;
}

/**
 * Was der aktive Ansichts-Chip sagt, wenn man ihn beruehrt.
 *
 * Nutzerbefund vom 2026-08-29: "galaxy Knopf macht nichts". W8b hat den Satz
 * ehrlich gemacht ("diese Ansicht laeuft schon"), und der Nutzer hat am Tag
 * darauf gesagt, was er stattdessen will: "die beiden Buttons unten links
 * sollten auch aufklappen und zuklappen koennen." Seit W10b tut der aktive Knopf
 * also etwas, und der Satz sagt beides: dass diese Ansicht schon dasteht, und
 * was ein Klick jetzt bewirkt.
 */
export function graphModeActiveTitle(mode: GraphMode): string {
    return `${mode} is already showing: click to fold the graph away`;
}

/** Was ein Chip sagt, solange die Sektion zugeklappt ist. */
export function graphModeCollapsedTitle(mode: GraphMode): string {
    return `open the graph again and show ${mode}`;
}

/**
 * Was die Einpassung sagt, und warum sie ein Knopf und keine Automatik ist.
 *
 * Die Kamera passt das Bild von selbst ein, sobald es entsteht oder die Flaeche
 * sich aendert (Entscheidung 18 im Kopf). Danach gehoert sie dem Leser: er
 * dreht, zieht und zoomt, und nichts darf ihm dabei ins Steuer greifen. Nach
 * zehn Sekunden Ziehen weiss aber niemand mehr, wie es angefangen hat, und
 * genau dafuer ist dieser Knopf da (Nutzerbefund 2026-08-30, AC5).
 */
export const GALAXY_FIT_LABEL = 'fit view';
export const GALAXY_FIT_TITLE =
    'put the whole graph back in the picture: the camera stands square on its widest side, '
    + 'far enough away that every node fits';

/** Der Griff, an dem der Beweislauf die Projektion anfasst. */
export interface AtlasHierarchySeam {
    /** Die Identitaet der Wurzel, also des gewaehlten Einstiegspunkts. */
    root: string;
    /** Ihr Anzeigename. */
    rootName: string;
    /** Wie viele Symbole im Bild stehen. */
    nodes: number;
    /** Wie viele Ebenen es hat, die Wurzel als erste. */
    depth: number;
    truncated: boolean;
    cap: number;
    walkDepth: number;
    /** Je Knoten: Schluessel, Name, Datei, Hop und die gezeichnete Position. */
    placements: { key: string; name: string; file: string; hop: number; x: number; y: number }[];
    /** Die gezeichneten Kanten, in den Schluesseln des Walks. */
    edges: { from: string; to: string }[];
    /** Wie viele Linien aus dem Walk stammen. */
    walkEdges: number;
    /** Wie viele Beziehungen die Layout-Antwort dazugelegt hat. */
    extraEdges: number;
    /** Je dazugelegte Beziehung: Art, Enden und Spur. */
    extras: { type: string; from: string; to: string; offset: number }[];
}

/** Der Griff, an dem der Beweislauf die Galaxie anfasst. */
export interface AtlasGalaxySeam {
    /** Wie viele Knoten geladen sind. Null heisst: noch keine Antwort. */
    nodes: number;
    /** Wie oft ein frisches Kameraziel gesetzt wurde, also wie oft geflogen wurde. */
    targetChanges: number;
    /** Wie viele Knoten gerade hervorgehoben sind. */
    highlightedCount: number;
    /** Der qualifizierte Name des zuletzt angeflogenen Knotens. */
    lastTargetQn: string;
    /**
     * Einen Knoten anklicken, ohne Maus.
     *
     * Ausdruecklich ein Testgriff und nichts anderes: ein Klick auf eine
     * WebGL-Szene ist ein Raycast, und ein Beweislauf, der auf Pixel zielt,
     * misst die Kameraposition und nicht den Klickpfad. Der Griff ruft genau
     * den Weg, den auch die Maus ruft, und faengt nichts ab. Er gilt fuer das
     * Bild, das gerade dasteht.
     */
    clickNode: (qualifiedName: string) => boolean;
    /** Ob die Legende gerade offen ist. */
    legendOpen: boolean;
    /** Wie viele Elemente sie erklaert. */
    legendEntries: number;
    /**
     * Die Bloom-Staerke, mit der die Szene gerade laeuft.
     *
     * Genau der Wert, den die Szene bekommt, und nicht ein zweiter daneben: der
     * Beweislauf liest daran, dass die Hierarchie ohne das Leuchten der Galaxie
     * gezeichnet wird.
     */
    bloom: number;
    /**
     * Die Namenskaesten, so wie die Szene sie wirklich gezeichnet hat, in
     * Weltkoordinaten.
     *
     * Gemeldet und nicht nachgerechnet: was ein Name einnimmt, weiss nur die
     * Ebene, die ihn in eine Textur schreibt. Alle Knoten dieser Ansicht liegen
     * auf z=0 und die Kamera steht frontal davor, also ist eine Ueberlappung
     * dieser Rechtecke genau eine Ueberlappung auf dem Schirm.
     */
    labelBoxes: LabelBox[];
    /** Welches Bild dasteht. */
    mode: GraphMode;
    /** Ob die Sektion aufgeklappt ist. Zugeklappt zeigt sie keines der Bilder. */
    open: boolean;
    /** Ob es einen Walk gibt, den man zeigen koennte. */
    hierarchyAvailable: boolean;
    /** Woher die Wurzel der Hierarchie kommt: aus einem Walk oder aus dem Fokus. */
    hierarchyOrigin: HierarchyRootOrigin | '';
    /**
     * Wie oft die Kamera das Bild eingepasst hat.
     *
     * Der Beweislauf liest daran, DASS eingepasst wurde; WO die Knoten danach
     * liegen, misst er an `globalThis.__atlasGalaxyFit` in der Szene selbst.
     */
    fits: number;
    /** Was bei der letzten Einpassung gerechnet wurde. Leer, solange keine lief. */
    lastFit: {
        mode: GraphMode;
        /** `principal` in der Galaxie, `frontal` in der flachen Hierarchie. */
        kind: string;
        aspect: number;
        nodes: number;
        distance: number;
        width: number;
        height: number;
        depth: number;
        normal: [number, number, number];
        up: [number, number, number];
    } | undefined;
    /** Genau der Satz, der im Kopf steht. */
    headline: string;
    /** Der Knoten, um den der Ring laeuft. Leer, wenn keiner. */
    pulsedQn: string;
    /** Die Projektion, so wie sie gezeichnet wuerde. Fehlt ohne Walk. */
    hierarchy: AtlasHierarchySeam | undefined;
    /**
     * Die Kantenarten des gezeigten Bildes, gezaehlt, mit Farbe und Lage.
     *
     * Genau die Liste, aus der die Legende ihre Zeilen macht: der Beweislauf
     * soll die Zeilen gegen die geladene Antwort halten koennen, ohne dass eine
     * zweite Zaehlung daneben entsteht.
     */
    edgeKinds: (EdgeKind & { hidden: boolean })[];
    /** Welche Arten gerade aus dem Bild genommen sind. */
    hiddenKinds: string[];
    /** Wie viele Kanten die Szene nach dem Filter wirklich bekommt. */
    drawnEdges: number;
    /** Die zweite Kopfzeile: woraus die Linien bestehen und was fehlt. */
    edgeNote: string;
}

/** Ein Akteur, so wie der Beweislauf ihn liest. */
export interface AtlasAgentSeamActor {
    id: string;
    name: string;
    you: boolean;
    color: string;
    letter: string;
    kind: string;
    kindLetter: string;
    placement: string;
    uncertain: boolean;
    nodeId: number;
    qualifiedName: string;
    placeName: string;
    why: string;
    ghosts: number[];
    testedNodeId: number;
    intent: string;
    count: number;
    missed: number;
    strip: number[];
    paths: string[];
    lastTool: string;
    lastPath: string;
    lastLines: number[];
    /** Der Winkel aus dem Bild, in dem dieser Griff geschrieben wurde. */
    angle: number;
    /** Ob dieser Akteur seit ueber einer Minute nichts geliefert hat. */
    idle: boolean;
    /** Wie lange sein letztes Ereignis her ist. */
    sinceMs: number;
    /** Wie viele Ereignisse er im Pulsfenster geliefert hat. */
    recentEvents: number;
    /** Die Dauer eines Atemzugs. `0` heisst: er atmet nicht. */
    pulseMs: number;
    /** Der Ausschlag des Pulses. `0` heisst derselbe Satz. */
    pulseAmplitude: number;
    /** Die Knoten seiner Spur, neuester zuerst. */
    trail: number[];
    /** Ob sein Koerper gezeichnet wird, oder ob der Deckel ihn zurueckhaelt. */
    drawn: boolean;
}

/** Der Griff, an dem der Beweislauf die Agentenebene anfasst. */
export interface AtlasAgentSeam {
    on: boolean;
    layerOn: boolean;
    sourceState: string;
    origin: string;
    /** Wie viele Anfragen an die Bruecke gingen. Aus heisst: null. */
    requests: number;
    drops: number;
    mode: string;
    file: string;
    error: string;
    port: number;
    events: number;
    missed: number;
    perMinute: number;
    unreadable: number;
    size: string;
    filter: string;
    follow: boolean;
    trails: boolean;
    /** Das Vollbild. Hiess bis W11b `cinema` (Nutzerwunsch 2026-08-30). */
    fullscreen: boolean;
    trailWindowMs: number;
    /** Wie viele Akteure der Umschalter gerade durchlaesst. */
    shown: number;
    /** Der Deckel gezeichneter Koerper, und was er zurueckhaelt. */
    cap: number;
    capped: number;
    drawn: number;
    /** Wie lange ein Ortswechsel dauert, in Millisekunden. */
    transitionMs: number;
    /** Welche teuren Wirkungen gerade eingeschaltet sind. */
    effects: { tails: boolean; trails: boolean; waves: boolean; timeline: boolean };
    /** Der Zeitstrahl, so wie er dasteht. */
    timeline: {
        mode: string;
        from: number;
        to: number;
        windowMs: number;
        tracks: number;
        ticks: number;
        shown: boolean;
        width: number;
    } | undefined;
    /** Wohin die FOLLOW-Kamera zuletzt geschickt wurde. */
    follow_: {
        nodeId: number;
        actor: string;
        position: [number, number, number];
        at: number;
    } | undefined;
    /** Die Bahnradien der gezeichneten Koerper. */
    radii: Record<string, number>;
    /** Die Weltposition jedes Koerpers aus dem zuletzt gezeichneten Bild. */
    positions: Record<string, { x: number; y: number; z: number }>;
    /** Wie gross jeder Koerper in jenem Bild gezeichnet wurde. */
    pulses: Record<string, number>;
    /** Wie viele Punkte der Schweif jedes Koerpers gerade traegt. */
    tails: Record<string, number>;
    /** Der letzte Flug je Akteur, mit allen aufgezeichneten Punkten. */
    motion: Record<string, unknown>;
    /** Wo die Kamera stand. */
    camera: { position: { x: number; y: number; z: number }; at: number };
    /** Die Zeichenreihenfolgen: die Spur, und die kleinste aller anderen. */
    renderOrders: {
        trail: number;
        others: number;
        objects: number;
        trails: number;
        dash: [number, number];
    };
    /** Die Schreib-Brueche, die gerade eine Welle tragen. */
    waves: { actor: string; key: string; nodeId: number; events: number; from: number; to: number }[];
    /** Die Zeilen des Ereignis-Tickers, woertlich. */
    ticker: {
        ts: number;
        actor: string;
        kind: string;
        place: string;
        lines: number[];
        text: string;
    }[];
    actors: AtlasAgentSeamActor[];
    unmapped: { ts: number; agent: string; tool: string; path: string; detail: string; why: string }[];
    /**
     * Die Winkel, LEBEND.
     *
     * Ein Verweis auf dieselbe Tabelle, die die Ebene in jedem Bild schreibt
     * (src/galaxy/AgentLayer.tsx). Die Zahl in `actors[].angle` ist die aus dem
     * Bild, in dem dieser Griff geschrieben wurde, und damit alt, sobald der
     * naechste Rahmen laeuft; wer die Bewegung messen will, liest hier.
     */
    angles: Record<string, number>;
}

declare global {
    // eslint-disable-next-line no-var
    var __atlasGalaxy: AtlasGalaxySeam | undefined;
    // eslint-disable-next-line no-var
    var __atlasAgents: AtlasAgentSeam | undefined;
}

export interface GalaxyPanelProps {
    /** Das Projekt, dessen Layout gezeigt wird. Leer heisst: nichts laden. */
    project: string;
    /** Ob das Panel im Layout sichtbar ist. */
    visible: boolean;
    /** Der qualifizierte Name des Twin-Subjekts, dem die Kamera folgt. */
    focusQualifiedName?: string | undefined;
    /** Der Anzeigename desselben Subjekts, fuer die ehrliche Fehlanzeige. */
    focusName?: string | undefined;
    /** Ein angeklickter Knoten mit Datei. Die App oeffnet ihn und folgt ihm. */
    onOpenNode: (node: GraphNode) => void;
    /** Was geladen wurde, fuer die Aufrufer, die es brauchen. */
    onLayout?: ((data: GraphData) => void) | undefined;
    /** Ersetzbares fetch, damit Tests ohne Netz laufen. */
    fetch?: typeof globalThis.fetch | undefined;
    /**
     * Der laufende Vorwaerts-Walk, wenn einer laeuft.
     *
     * Der Walk selbst und nicht die daraus gebaute Fuehrung: die Fuehrung hat
     * die Symbole ohne Datei schon weggelassen, weil ein Schritt, den man nicht
     * oeffnen kann, kein Schritt ist. Fuer das Bild gilt das nicht, dort ist so
     * ein Symbol ein Punkt wie jeder andere, und ihn wegzulassen hiesse, eine
     * Kette kuerzer zu zeichnen, als der Index sie kennt.
     */
    walk?: ClosureResult | undefined;
    /**
     * Der Vorwaerts-Walk aus dem Symbol im Fokus (W10b, AC3).
     *
     * Von aussen und nicht hier gerechnet: ein Closure braucht den Provider, und
     * den hat die App. Er gilt nur, solange kein echter Walk laeuft; ein
     * Einstiegs-Spaziergang ist eine Entscheidung des Lesers und schlaegt einen
     * Ort, an dem er zufaellig steht.
     */
    focusWalk?: ClosureResult | undefined;
    /**
     * Der Zaehler, mit dem die App eine Einpassung anfordert (W10b, AC5).
     *
     * Eine Zahl und kein Rueckruf, weil es eine Aufforderung ist und kein
     * Zustand: "reset layout" bringt jede Zone auf ihre Vorgabe zurueck, und
     * seit W10b gehoert die eingepasste Ansicht des Graphen dazu. Jede neue Zahl
     * ist eine neue Aufforderung.
     */
    refit?: number;
    /**
     * Der qualifizierte Name des Schrittes, auf dem die Fuehrung gerade steht.
     *
     * Nur als Rueckfall: der Ring folgt dem Symbol vor dem Leser, und das ist
     * `focusQualifiedName`. Steht dort etwas, das gar nicht im Walk vorkommt,
     * sagt dieser Wert trotzdem noch, wo die Fuehrung steht.
     */
    stepQualifiedName?: string | undefined;
    /**
     * Der Speicher fuer den Klappzustand der Legende.
     *
     * Von aussen setzbar, damit ein Test ihn ersetzen kann, ohne globale
     * Objekte zu verbiegen. Fehlt er, wird der localStorage dieses Fensters
     * genommen; gibt es auch den nicht, gilt der Vorgabewert und die
     * Entscheidung haelt eine Sitzung lang.
     */
    legendStore?: Storage | undefined;
    /**
     * Das Panel zu- und wieder aufklappen.
     *
     * Von aussen, weil derselbe Zustand am [a]tlas-Menuepunkt haengt und zwei
     * Schalter fuer eine Lage zwei Lagen waeren. Fehlt der Rueckruf, steht kein
     * Schalter im Kopf: ein Knopf, der nichts tut, ist schlimmer als keiner.
     */
    onToggleVisible?: (() => void) | undefined;
    /**
     * Was der Leser im Einstellungen-Panel eingestellt hat (W10).
     *
     * Von aussen, weil es von aussen entschieden wird: der Nutzerwunsch vom
     * 2026-08-29 war ausdruecklich, dass alles, was Rechenzeit kostet, an EINEM
     * Ort steht. Ein zweiter Satz Schalter hier waere genau der zweite Ort.
     * Fehlt die Angabe, zeichnet das Panel wie vor W10.
     */
    display?: GraphDisplaySettings | undefined;
    /**
     * Der laufende Ereignisstrom der Agenten (W11a).
     *
     * Fehlt er, gibt es weder Koerper noch Instrument, und das Panel zeichnet
     * genau wie vor W11a. Er wird von aussen gefuehrt, weil der Live-Modus eine
     * Entscheidung des ganzen Fensters ist (Menue und Kommandozeile) und nicht
     * eine dieses Panels.
     */
    agents?: AgentsRuntime | undefined;
    /**
     * Ob die Agentenebene ueberhaupt gezeichnet wird.
     *
     * Aus der Darstellungs- und Leistungsgruppe der Einstellungen (W10 AC9), wie
     * jeder andere Schalter, der Rechenzeit kostet. Aus heisst: keine Koerper,
     * keine Pings, keine Linien; das Instrument bleibt und sagt es.
     */
    agentLayer?: boolean;
    /**
     * Was von den teuren Wirkungen der Agentenebene gezeichnet wird (W11b AC7b).
     *
     * Aus derselben Gruppe des Einstellungen-Panels wie `agentLayer`, und aus
     * demselben Grund: was Rechenzeit kostet, steht an EINEM Ort. Fehlt die
     * Prop, ist alles an.
     */
    agentEffects?: {
        tails: boolean;
        trails: boolean;
        waves: boolean;
        timeline: boolean;
    } | undefined;
    /**
     * Ob eine andere Flaeche die Escape-Taste gerade braucht.
     *
     * Die App weiss, was ueber dem Panel liegt (Hilfe, Einstiegsdialog,
     * Suchfenster, Einstellungen); dieses Panel weiss es nicht. Ohne die Auskunft
     * naehme der Vollbildmodus die Taste an sich und stuende damit VOR Flaechen,
     * die es laenger gibt. Die Reihenfolge ist eine Zusicherung dieser
     * Oberflaeche, und sie bleibt gueltig.
     */
    escapeTaken?: boolean;
    /**
     * Der Zaehler, mit dem die Kommandozeile das Vollbild umlegt (W11b).
     *
     * Dieselbe Form wie `refit`: die Wahl liegt hier, weil sie hier gespeichert
     * wird; was von aussen kommt, ist die Bitte.
     */
    fullscreenToggle?: number;
    /** Der Speicher fuer die Lage des Instruments. Ersetzbar fuer Tests. */
    agentStore?: Storage | undefined;
}

/** Wie lange die Zeitangaben im Instrument stehen, bis sie neu gerechnet werden. */
export const AGENT_TICK_MS = 1000;

export default function GalaxyPanel(props: GalaxyPanelProps): JSX.Element {
    const {
        project,
        visible,
        focusQualifiedName,
        focusName,
        onOpenNode,
        onLayout,
        walk,
        stepQualifiedName,
    } = props;

    const [data, setData] = useState<GraphData | undefined>(undefined);
    const [error, setError] = useState('');
    const [note, setNote] = useState(GALAXY_NO_FOCUS_NOTE);
    const [highlighted, setHighlighted] = useState<Set<number> | null>(null);
    const [cameraTarget, setCameraTarget] = useState<CameraTarget | null>(null);

    /*
     * Die Wahl des Lesers, oder nichts.
     *
     * Nichts heisst "noch nichts gewaehlt" und nicht "galaxy": nur so kann die
     * Vorgabe der Lage folgen (Walk da: hierarchy, sonst galaxy), ohne eine
     * getroffene Entscheidung zu ueberschreiben.
     */
    const [chosenMode, setChosenMode] = useState<GraphMode | undefined>(undefined);

    /*
     * Die Kantenarten, die der Leser aus dem Bild genommen hat (W9).
     *
     * Im Zustand dieses Panels und nicht im Speicher des Browsers: eine
     * ausgeblendete Art ist eine Frage an DIESES Bild ("wie sieht es ohne die
     * Definitionen aus"), keine Einstellung. Sie haelt die Sitzung und den
     * Wechsel zwischen den beiden Ansichten, und sie ist nach einem Reload
     * wieder weg, damit niemand vor einem Bild sitzt, dem ohne sein Wissen
     * etwas fehlt.
     */
    const [hiddenKinds, setHiddenKinds] = useState<ReadonlySet<string>>(() => new Set<string>());

    const toggleKind = useCallback((type: string) => {
        setHiddenKinds((hidden) => {
            const next = new Set(hidden);
            if (!next.delete(type)) {
                next.add(type);
            }
            return next;
        });
    }, []);

    /*
     * Die Zeichenflaeche, gemessen statt geraten.
     *
     * Die Rahmung der Hierarchie braucht das Seitenverhaeltnis: ein breites
     * Panel braucht fuer dieselbe Breite weniger Abstand als ein schmales. Ein
     * fester Wert waere eine Kamera, die nur bei einer Fensterbreite stimmt.
     */
    const scene = useRef<HTMLDivElement | null>(null);
    const panel = useRef<HTMLElement | null>(null);
    const [aspect, setAspect] = useState(1.3);
    /*
     * Die Breite der Zeichenflaeche, in Pixeln.
     *
     * Der Zeitstrahl haengt daran und nicht an einem Modus: unter
     * {@link TIMELINE_MIN_WIDTH} Pixeln waere eine Spur ueber fuenfzehn Minuten
     * ein Strich, in dem kein Ereignis mehr von seinem Nachbarn zu unterscheiden
     * ist, und eine Anzeige, die genauer aussieht als das Bild, das sie zeigt,
     * ist eine Behauptung. Gemessen statt geraten, damit die Grenze nachlesbar
     * ist.
     */
    const [sceneWidth, setSceneWidth] = useState(0);
    const labelBoxes = useRef<LabelBox[]>([]);
    const onLabelLayout = useCallback((boxes: LabelBox[]) => {
        labelBoxes.current = boxes;
    }, []);

    useEffect(() => {
        const node = scene.current;
        if (node === null) {
            return;
        }
        const measure = (): void => {
            setSceneWidth(node.clientWidth);
            if (node.clientHeight > 0) {
                setAspect(node.clientWidth / node.clientHeight);
            }
        };
        measure();
        if (typeof ResizeObserver === 'undefined') {
            return;
        }
        const observer = new ResizeObserver(measure);
        observer.observe(node);
        return () => observer.disconnect();
    }, []);

    /*
     * Der Klappzustand der Legende, einmal aus dem Speicher gelesen.
     *
     * Als Initialisierer und nicht als Effekt: ein Effekt wuerde die Legende
     * beim ersten Bild in der Vorgabelage zeigen und im zweiten umschalten,
     * und ein Kasten, der beim Laden von selbst zuklappt, sieht aus wie ein
     * Fehler.
     */
    const legendStore = props.legendStore
        ?? (typeof localStorage === 'undefined' ? undefined : localStorage);
    const [legendOpen, setLegendOpen] = useState(() => readLegendOpen(legendStore));

    const toggleLegend = useCallback(() => {
        setLegendOpen((open) => {
            writeLegendOpen(legendStore, !open);
            return !open;
        });
    }, [legendStore]);

    /*
     * Die Kante der Legende, gemessen: steht ueber oder unter dem Kasten noch
     * etwas?
     *
     * Der Befund aus dem Beweisbild von W9 (Bernhard, 2026-08-29): der Kasten
     * ist niedriger als sein Inhalt, sein Bildlauf ist auf dieser Maschine eine
     * ueberlagernde Leiste, die im Ruhezustand unsichtbar ist, und damit endete
     * der letzte sichtbare Satz mitten im Wort an einer harten Kante. Das sieht
     * aus wie ein Darstellungsfehler und nicht wie ein Anfang.
     *
     * Gemessen und nicht geraten, weil beide Antworten falsch waeren, wenn man
     * sie fest verdrahtet: ein Hinweis, der immer dasteht, luegt bei einer
     * kurzen Legende, und einer, der nie dasteht, ist der Befund. Der Zustand
     * haengt an drei Dingen, und alle drei koennen sich ohne die anderen
     * aendern: am Bildlauf (der Leser scrollt), an der Groesse des Kastens (das
     * Fenster aendert sich) und am Inhalt (die Ansicht wechselt, eine Art
     * kommt dazu).
     */
    const legendBox = useRef<HTMLDivElement | null>(null);
    const [legendEdge, setLegendEdge] = useState({ above: false, below: false });

    const measureLegendEdge = useCallback(() => {
        const node = legendBox.current;
        if (node === null) {
            return;
        }
        const above = node.scrollTop > 1;
        const below = node.scrollTop + node.clientHeight < node.scrollHeight - 1;
        setLegendEdge((edge) =>
            (edge.above === above && edge.below === below ? edge : { above, below }));
    }, []);

    /* ------------------------------------------------ die Agentenebene (W11a) */

    /*
     * Die Lage des Instruments, einmal aus dem Speicher gelesen.
     *
     * Als Initialisierer und nicht als Effekt, aus demselben Grund wie bei der
     * Legende: ein Kasten, der beim Laden von selbst zuklappt, sieht aus wie ein
     * Fehler.
     */
    const agentStore = props.agentStore
        ?? (typeof localStorage === 'undefined' ? undefined : localStorage);
    const [agentPreference, setAgentPreference] = useState<AgentsPreference>(
        () => loadAgentsPreference(agentStore, project),
    );
    const changeAgentPreference = useCallback((patch: Partial<AgentsPreference>) => {
        setAgentPreference((current) =>
            saveAgentsPreference(agentStore, project, { ...current, ...patch }));
    }, [agentStore, project]);

    /*
     * Und noch einmal, wenn das Projekt ankommt.
     *
     * Der Initialisierer oben laeuft beim ersten Bild, und da steht der
     * Projektname noch nicht immer fest (er kommt aus der Adresszeile und wird
     * von der App durchgereicht). Der Schluessel haengt aber am Projekt: ohne
     * dieses zweite Lesen bekaeme ein Leser, der das Instrument eingeklappt hat,
     * nach dem Reload wieder die Vorgabe, und die gespeicherte Entscheidung
     * waere eine, die niemand je zurueckliest.
     */
    useEffect(() => {
        if (project.length === 0) {
            return;
        }
        setAgentPreference(loadAgentsPreference(agentStore, project));
    }, [agentStore, project]);

    /*
     * Die Bitte der Kommandozeile um das Vollbild.
     *
     * Am Zaehler und nicht am Wert: zwei Orte fuer denselben Zustand waeren zwei
     * Wahrheiten darueber, ob das Bild gerade das ganze Fenster fuellt. Der
     * erste Wert zaehlt nicht als Bitte, sonst legte jedes Laden den Modus um.
     */
    const fullscreenAsked = props.fullscreenToggle ?? 0;
    const lastFullscreenAsk = useRef(fullscreenAsked);
    useEffect(() => {
        if (fullscreenAsked === lastFullscreenAsk.current) {
            return;
        }
        lastFullscreenAsk.current = fullscreenAsked;
        setAgentPreference((current) =>
            saveAgentsPreference(agentStore, project, {
                ...current,
                fullscreen: !current.fullscreen,
            }));
    }, [fullscreenAsked, agentStore, project]);

    /*
     * Die Uhr des Instruments.
     *
     * Die Zeitangaben ("here for 12s") und der Aktivitaetsstreifen der letzten
     * dreissig Sekunden haengen an der Gegenwart und nicht an einem Ereignis.
     * Ohne einen eigenen Takt blieben sie stehen, sobald der Strom eine Pause
     * macht, und ein Streifen, der beim Schweigen einfriert, waere ein Bild
     * ueber eine Zeit, die nicht vergangen ist. Der Takt laeuft nur, solange es
     * ueberhaupt einen Strom gibt.
     */
    const [agentNow, setAgentNow] = useState(() => Date.now());
    const agentsOn = props.agents !== undefined;

    /*
     * Ob der Live-Modus AN ist.
     *
     * Zwei verschiedene Fragen, und sie duerfen nicht zusammenfallen:
     * `props.agents` heisst "es gibt einen Strom, den man fuehren koennte",
     * `props.agents.on` heisst "der Leser sieht gerade zu". Ist der Modus aus,
     * gibt es weder Koerper noch Instrument: es gibt nichts zu erklaeren, und
     * ein Kasten in der Ecke, der bei jedem Laden "der Live-Modus ist aus"
     * sagt, nimmt dem Graphen dauerhaft eine Ecke fuer eine Auskunft, die schon
     * im Menue steht.
     */
    const liveOn = props.agents?.on === true;
    useEffect(() => {
        if (!agentsOn) {
            return;
        }
        setAgentNow(Date.now());
        const timer = window.setInterval(() => setAgentNow(Date.now()), AGENT_TICK_MS);
        return () => window.clearInterval(timer);
    }, [agentsOn]);

    // Der Zaehler und der letzte Zielname wandern durch Refs, weil sie
    // Beobachtungen ueber den Ablauf sind und nichts zeichnen.
    const targetChanges = useRef(0);
    const lastTargetQn = useRef('');

    // Einmal sichtbar, immer gemountet: siehe Entscheidung 2 im Kopf.
    const everVisible = useRef(false);
    if (visible) {
        everVisible.current = true;
    }

    const fetchImpl = props.fetch;

    useEffect(() => {
        if (project.length === 0) {
            return;
        }
        let cancelled = false;
        setError('');
        loadLayout(project, fetchImpl === undefined ? {} : { fetch: fetchImpl })
            .then((loaded) => {
                if (cancelled) {
                    return;
                }
                setData(loaded);
                if (onLayout !== undefined) {
                    onLayout(loaded);
                }
            })
            .catch((failure: unknown) => {
                if (cancelled) {
                    return;
                }
                setData(undefined);
                setError(failure instanceof Error ? failure.message : String(failure));
            });
        return () => {
            cancelled = true;
        };
    }, [project, fetchImpl, onLayout]);

    /*
     * Der Walk als Bild.
     *
     * Haengt am Walk UND am geladenen Layout, weil Farbe und Groesse von dort
     * kommen, wo das Layout das Symbol kennt. Kommt das Layout spaeter an,
     * faerbt sich das Bild nach und die Kamera rahmt es noch einmal; die
     * Positionen aendern sich dabei nicht, die haengen nur am Walk.
     */
    /*
     * Der Walk, aus dem das Bild entsteht: der echte, sonst der aus dem Fokus.
     *
     * In dieser Reihenfolge, und das ist Entscheidung 17 im Kopf: ein
     * Einstiegs-Spaziergang ist eine Entscheidung ("hier fange ich an"), ein
     * Fokus ist ein Ort ("hier stehe ich gerade"). Laeuft ein Spaziergang, ist
     * er gemeint, auch wenn der Leser darin gerade woanders hinsieht.
     */
    const activeWalk = walk ?? props.focusWalk;
    const hierarchyOrigin: HierarchyRootOrigin = walk === undefined ? 'focus' : 'walk';

    const projection = useMemo(
        () => (activeWalk === undefined ? undefined : projectHierarchy(activeWalk, { layout: data })),
        [activeWalk, data],
    );

    /*
     * Was dasteht, wenn niemand gewaehlt hat.
     *
     * Ein WALK schaltet um, ein Fokus nicht. Wer einen Einstiegspunkt gewaehlt
     * hat, hat nach der Tiefe gefragt, und die Wolke antwortet nicht darauf
     * (Entscheidung 7). Ein Fokus dagegen entsteht bei jedem Klick in den Code;
     * eine Ansicht, die dabei von selbst wechselt, waere ein Bild, das niemand
     * bestellt hat, und der Leser haette den Ueberblick verloren, ohne etwas
     * dafuer zu tun.
     */
    const mode: GraphMode = projection === undefined
        ? 'galaxy'
        : chosenMode ?? (walk === undefined ? 'galaxy' : 'hierarchy');

    /*
     * Was der Index ausser den Aufrufen zwischen den gezeigten Symbolen kennt.
     *
     * Haengt an der Projektion UND am geladenen Layout, weil es von dort kommt.
     * Ohne Layout ist die Liste leer, und die Hierarchie zeigt genau das, was
     * sie vor W9 gezeigt hat.
     */
    const indexEdges = useMemo(
        () => (projection === undefined ? [] : hierarchyIndexEdges(projection, data)),
        [projection, data],
    );

    /*
     * Das Bild, bevor der Filter darueber geht.
     *
     * Es ist die Grundlage fuer drei Dinge, die alle dieselbe Antwort brauchen:
     * die Legende (was gibt es), die Nachbarschaft (wer haengt woran) und der
     * Kopf (wie viel steht da). Die Szene bekommt danach die gefilterte
     * Fassung, und nur sie.
     */
    const picture = useMemo(() => {
        if (mode !== 'hierarchy' || projection === undefined) {
            return data;
        }
        return indexEdges.length === 0
            ? projection.data
            : { ...projection.data, edges: [...projection.data.edges, ...indexEdges] };
    }, [mode, projection, indexEdges, data]);

    const kinds = useMemo(() => edgeKinds(picture), [picture]);
    const hiddenHere = kinds.filter((kind) => hiddenKinds.has(kind.type)).length;
    const kindNote = edgeKindNote(kinds.length, hiddenHere);

    const shown = useMemo(
        () => (picture === undefined ? undefined : withoutEdgeKinds(picture, hiddenKinds)),
        [picture, hiddenKinds],
    );

    const index = useMemo(
        () => (picture === undefined ? new Map<string, GraphNode>() : nodesByQualifiedName(picture.nodes)),
        [picture],
    );

    // Die Legende haengt am gezeigten Bild: ihre Kantenarten nennen die dieses
    // Graphen und nicht alle, die die Tabelle kennt.
    const legend = useMemo(
        () => (mode === 'hierarchy'
            ? hierarchyLegendEntries(picture)
            : galaxyLegendEntries(picture)),
        [mode, picture],
    );

    // Der Inhalt hat sich geaendert (aufgeklappt, Ansicht gewechselt, eine Art
    // mehr): die Kante gilt neu. Siehe measureLegendEdge.
    useEffect(() => {
        measureLegendEdge();
    }, [measureLegendEdge, legendOpen, legend, hiddenKinds]);

    // Und wenn der Kasten selbst seine Hoehe aendert, weil das Fenster es tut.
    useEffect(() => {
        const node = legendBox.current;
        if (node === null || typeof ResizeObserver === 'undefined') {
            return;
        }
        const observer = new ResizeObserver(measureLegendEdge);
        observer.observe(node);
        return () => observer.disconnect();
    }, [measureLegendEdge, legendOpen]);

    /*
     * Die eigene Navigation des Lesers, als Ereignis.
     *
     * Sie entsteht HIER und geht nirgendwo hin: die Bruecke kennt keine Route,
     * die etwas entgegennimmt, und dieses Ereignis verlaesst das Fenster nie.
     * Es traegt darum auch keinen Lauf einer fremden Aufzeichnung, sondern einen
     * eigenen, und seine Nummer zaehlt in diesem Fenster.
     */
    const youSeq = useRef(0);
    const pushEvent = props.agents?.push;
    useEffect(() => {
        if (pushEvent === undefined || !liveOn) {
            return;
        }
        if (focusQualifiedName === undefined || focusQualifiedName.length === 0) {
            return;
        }
        const node = index.get(focusQualifiedName);
        if (node === undefined) {
            return;
        }
        youSeq.current += 1;
        pushEvent({
            ts: Date.now(),
            agent: YOU_ID,
            run: 'this-window',
            seq: youSeq.current,
            phase: 'end',
            tool: 'Open',
            path: node.file_path ?? '',
            ...(node.start_line !== undefined && node.end_line !== undefined
                ? { lines: [node.start_line, node.end_line] as const }
                : {}),
            detail: node.qualified_name ?? node.name,
            source: 'ui',
            replay: false,
        }, true);
    }, [pushEvent, liveOn, focusQualifiedName, index]);

    /*
     * Die Verortung, EINMAL gerechnet.
     *
     * Aus demselben Ergebnis zeichnet die Ebene ihre Koerper und schreibt das
     * Instrument seine Zeilen. Zwei Rechnungen waeren zwei Wahrheiten ueber
     * dasselbe Bild, und die Stelle, an der sie auseinanderlaufen, faellt
     * niemandem auf.
     */
    const placementIndex = useMemo(
        () => buildPlacementIndex(picture?.nodes ?? []),
        [picture],
    );
    /*
     * Der Zeitstrahl und seine zwei Haltepunkte (W11b AC4).
     *
     * `pausedAt` haelt das FENSTER an: die Ereignisse laufen weiter ein, sie
     * stehen nach dem Fortsetzen alle da, und was steht, ist das Nachlaufen.
     * `replayAt` ist der staerkere Griff: der Leser hat auf eine Stelle
     * geklickt und sieht den Zustand von damals, und dann steht die GANZE
     * Ansicht auf jenem Zeitpunkt, sichtbar gekennzeichnet. Ein alter Zustand
     * ohne Kennzeichnung waere die gefaehrlichste Anzeige dieser Oberflaeche.
     */
    const [pausedAt, setPausedAt] = useState<number | undefined>(undefined);
    const [replayAt, setReplayAt] = useState<number | undefined>(undefined);
    useEffect(() => {
        if (!liveOn) {
            setPausedAt(undefined);
            setReplayAt(undefined);
        }
    }, [liveOn]);

    const agentsView = useMemo(() => {
        const runtime = props.agents;
        if (runtime === undefined) {
            return undefined;
        }
        const at = replayAt ?? agentNow;
        return buildAgentsView({
            state: replayAt === undefined ? runtime.state : stateUntil(runtime.state, replayAt),
            nodes: picture?.nodes ?? [],
            now: at,
            filter: agentPreference.filter,
            trailWindowMs: agentPreference.trailWindowMs,
            index: placementIndex,
            cap: DRAWN_BODIES_CAP,
        });
    }, [props.agents, picture, agentNow, replayAt, agentPreference.filter,
        agentPreference.trailWindowMs, placementIndex]);

    /*
     * Der Zeitstrahl selbst, aus derselben Sicht wie alles andere.
     *
     * Er nimmt ALLE aktiven Akteure und nicht die gefilterten: der Filter
     * you/agents/both entscheidet, wessen Koerper man sieht, und eine Spur, die
     * dabei verschwindet, waere eine Luecke in der Zeit statt einer Auswahl im
     * Bild. Was er zeigt, sind die behaltenen Ereignisse; mehr hat dieses
     * Fenster nicht.
     */
    const timeline = useMemo(() => {
        if (agentsView === undefined) {
            return undefined;
        }
        const stored = props.agents?.state.actors ?? [];
        return buildTimeline({
            actors: agentsView.all.map((actor) => {
                const source = stored.find((entry) => entry.id === actor.id);
                return {
                    id: actor.id,
                    name: actor.name,
                    color: actor.color,
                    letter: actor.letter,
                    you: actor.you,
                    idle: actor.idle,
                    firstTs: source?.firstTs ?? actor.lastTs,
                    events: (source?.events ?? []).map((event) => ({
                        ts: event.ts,
                        kind: workKindOf(event.tool, event.detail),
                    })),
                };
            }),
            now: agentNow,
            windowMs: agentPreference.trailWindowMs,
            ...(pausedAt === undefined ? {} : { pausedAt }),
            ...(replayAt === undefined ? {} : { replayAt }),
        });
    }, [agentsView, props.agents, agentNow, agentPreference.trailWindowMs, pausedAt, replayAt]);

    /*
     * Und die zweite Bedingung: `agentLayer` kommt aus den Einstellungen und
     * heisst "diese Ebene kostet mir zu viel". Sie ist von `liveOn` getrennt,
     * weil sie etwas anderes sagt: der Modus laeuft weiter, das Instrument
     * bleibt stehen und sagt, dass die Ebene abgeschaltet ist.
     */
    const agentLayerOn = props.agentLayer !== false;

    /*
     * Ob der Zeitstrahl gezeigt wird.
     *
     * Drei Bedingungen, und alle drei sind Auskuenfte: der Live-Modus laeuft,
     * der Schalter in den Einstellungen ist an, und die Zeichenflaeche ist breit
     * genug, dass ein Strich von seinem Nachbarn zu unterscheiden ist. Die
     * dritte ist gemessen und nicht an einen Modus gebunden; welche Breite es
     * braucht und warum, steht bei {@link TIMELINE_MIN_WIDTH}.
     */
    const timelineShown = liveOn
        && agentLayerOn
        && props.agentEffects?.timeline !== false
        && sceneWidth >= TIMELINE_MIN_WIDTH;

    /** Ob der Graph gerade das ganze Fenster fuellt. */
    const fullscreen = liveOn && agentPreference.fullscreen;

    /*
     * Der Rahmen wechselt, das Bild bleibt (W11b AC5).
     *
     * Das Umschalten in das Vollbild und zurueck aendert die Groesse der
     * Zeichenflaeche, und auf eine neue Groesse passt dieses Panel sonst das
     * Bild ein (Entscheidung 18). Fuer DIESEN Groessenwechsel gilt das nicht:
     * das Vollbild ist derselbe Graph in einem anderen Rahmen, und AC5 verlangt
     * ausdruecklich, dass die Kameralage dabei erhalten bleibt. Eine Einpassung
     * beim Verlassen waere eine Kamera, die den Leser dorthin zurueckwirft, wo
     * er stand, bevor er selbst gefahren ist.
     *
     * Gemerkt wird genau EINE Aspektaenderung: die, die der Rahmenwechsel
     * ausloest. Jede andere passt weiter ein.
     */
    const lastFrame = useRef(fullscreen);
    const skipNextFit = useRef(false);
    useEffect(() => {
        if (lastFrame.current === fullscreen) {
            return;
        }
        lastFrame.current = fullscreen;
        skipNextFit.current = true;
    }, [fullscreen]);

    /**
     * Auf eine Menge Knoten zufliegen.
     *
     * Immer ein frisches Ziel, auch fuer dieselbe Menge: der CameraAnimator der
     * Uebernahme startet an der Objekt-Identitaet.
     */
    const flyTo = useCallback(
        (
            nodes: GraphNode[],
            ids: Set<number>,
            qualifiedName: string,
            /*
             * Mit Feder statt mit Anflug (W11b). Nur die FOLLOW-Kamera setzt es;
             * jede andere Fahrt dieses Panels bleibt der Anflug, den die
             * Beweisbilder bis W11a zeigen.
             */
            spring = false,
        ): CameraTarget | null => {
            const target = computeCameraTarget(nodes, ids);
            if (target === null) {
                return null;
            }
            const next = spring ? { ...target, spring: true } : target;
            setCameraTarget(next);
            targetChanges.current += 1;
            lastTargetQn.current = qualifiedName;
            return next;
        },
        [],
    );

    /** Wohin die FOLLOW-Kamera zuletzt geschickt wurde. Fuer die Messung. */
    const followGoal = useRef<{
        nodeId: number;
        actor: string;
        position: [number, number, number];
        at: number;
    } | undefined>(undefined);

    /*
     * Die Einpassung (W10b, AC5): das ganze Bild, mit Rand, ohne Anflug.
     *
     * Sie laeuft, wenn ein Bild ENTSTEHT (neues Layout, andere Ansicht, andere
     * Projektion), wenn die Zeichenflaeche eine andere Groesse bekommt und wenn
     * der Leser darum bittet (`refit`, der Knopf und "reset layout"). Sie laeuft
     * NICHT, wenn der Leser die Kamera bewegt, wenn er eine Kantenart aus dem
     * Bild nimmt oder wenn der Fokus wandert: das sind seine Bewegungen, und
     * eine Kamera, die dabei zurueckspringt, nimmt ihm die Ansicht aus der Hand.
     *
     * Zwei Rahmungen, und der Unterschied steht in Entscheidung 18: die Galaxie
     * ist eine dreidimensionale Wolke und braucht auch eine RICHTUNG (senkrecht
     * auf die groesste Flaeche, `computeFitTarget`); die Hierarchie ist eine
     * flache Zeichnung mit Spalten und bleibt bei der frontalen Rahmung aus W5c,
     * damit ihr Raster waagerecht bleibt. Gerahmt wird dort das Rechteck aus
     * `hierarchyFrame`, also samt dem Platz, den die Namen neben den Punkten
     * brauchen.
     */
    const fitCount = useRef(0);
    const lastFit = useRef<AtlasGalaxySeam['lastFit']>(undefined);
    const requestedFit = props.refit ?? 0;
    const [ownFit, setOwnFit] = useState(0);
    const refitNow = useCallback(() => setOwnFit((count) => count + 1), []);

    useEffect(() => {
        if (!visible) {
            return;
        }
        /* Der Rahmenwechsel des Vollbilds passt nicht ein. Siehe `skipNextFit`. */
        if (skipNextFit.current) {
            skipNextFit.current = false;
            return;
        }
        if (mode === 'hierarchy' && projection !== undefined) {
            const box = hierarchyFrame(projection);
            const target = computeFrameTarget(box, aspect);
            setCameraTarget(target);
            targetChanges.current += 1;
            fitCount.current += 1;
            lastTargetQn.current = projection.rootKey;
            lastFit.current = {
                mode,
                kind: 'frontal',
                aspect,
                nodes: projection.data.nodes.length,
                distance: target.position.z - target.lookAt.z,
                width: box.width,
                height: box.height,
                depth: 0,
                normal: [0, 0, 1],
                up: [0, 1, 0],
            };
            return;
        }
        const nodes = picture?.nodes ?? [];
        if (nodes.length === 0) {
            return;
        }
        const target = computeFitTarget(nodes, aspect);
        if (target === null) {
            return;
        }
        setCameraTarget(target);
        targetChanges.current += 1;
        fitCount.current += 1;
        const fit = target.fit;
        lastFit.current = {
            mode,
            kind: 'principal',
            aspect,
            nodes: fit.counted,
            distance: fit.distance,
            width: fit.width,
            height: fit.height,
            depth: fit.depth,
            normal: [fit.normal.x, fit.normal.y, fit.normal.z],
            up: [fit.up.x, fit.up.y, fit.up.z],
        };
    }, [visible, mode, projection, picture, aspect, requestedFit, ownFit]);

    // Hin-Richtung in der Galaxie: das Twin-Subjekt zieht die Kamera nach.
    useEffect(() => {
        if (mode !== 'galaxy' || data === undefined) {
            return;
        }
        if (focusQualifiedName === undefined || focusQualifiedName.length === 0) {
            return;
        }
        const node = index.get(focusQualifiedName);
        if (node === undefined) {
            setNote(missingNodeNote(focusName ?? focusQualifiedName));
            return;
        }
        const ids = neighbourIds(node.id, data.edges);
        setHighlighted(ids);
        flyTo(data.nodes, ids, focusQualifiedName);
        setNote('');
        // `focusName` steht bewusst nicht in der Liste: er begleitet den
        // qualifizierten Namen und darf keine zweite Kamerafahrt ausloesen.
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [mode, data, index, focusQualifiedName, flyTo]);

    /*
     * FOLLOW: die Kamera geht dorthin, wo sich zuletzt etwas bewegt hat.
     *
     * Sie faehrt nur, wenn der Ort WECHSELT. Ein Anflug bei jedem Ereignis waere
     * eine Kamera, die bei zehn Aenderungen an derselben Datei zehnmal
     * losfliegt, und die Bewegung waere Zittern und keine Auskunft. Und sie
     * faehrt nur, solange der Schalter an ist: eine Kamera, die von selbst
     * losfaehrt, nimmt dem Leser die Ansicht, die er gerade eingestellt hat.
     */
    const followed = useRef(-1);
    const [followLine, setFollowLine] = useState<ActorView | undefined>(undefined);
    useEffect(() => {
        if (!liveOn || !agentPreference.follow || agentsView === undefined
            || picture === undefined) {
            return;
        }
        const target = agentsView.actors.find((actor) => actor.node !== undefined);
        const node = target?.node;
        if (node === undefined || node.id === followed.current) {
            return;
        }
        followed.current = node.id;
        /*
         * Mit der Feder (W11b AC3c). Die Kamera bekommt hier waehrend eines
         * Anfluges ein neues Ziel, sobald der Agent weiterzieht; der Anflug der
         * Szene faengt bei jedem Ziel wieder bei null an, und genau das ist der
         * Ruck, den dieser Zyklus verbietet. Die Feder traegt ihre
         * Geschwindigkeit ueber das neue Ziel hinweg und schwingt nicht ueber.
         */
        const goal = flyTo(
            picture.nodes,
            new Set([node.id]),
            target?.placement.qualifiedName ?? node.name,
            true,
        );
        followGoal.current = goal === null
            ? undefined
            : {
                nodeId: node.id,
                actor: target?.id ?? '',
                position: [goal.position.x, goal.position.y, goal.position.z],
                at: Date.now(),
            };
        setFollowLine(target);
    }, [liveOn, agentPreference.follow, agentsView, picture, flyTo]);

    /*
     * Die Ereigniszeile, die kurz eingeblendet wird.
     *
     * Sie nennt vier Dinge und alle vier sind gemessen: wer, welche Art von
     * Arbeit, welches Symbol, welche Zeilen. Sie verschwindet nach ein paar
     * Sekunden wieder, weil sie zum Anflug gehoert und nicht zum Bild; eine
     * Zeile, die stehen bleibt, waere eine Auskunft ueber eine Bewegung, die
     * laengst vorbei ist.
     */
    useEffect(() => {
        if (followLine === undefined) {
            return;
        }
        const timer = window.setTimeout(() => setFollowLine(undefined), FOLLOW_LINE_MS);
        return () => window.clearTimeout(timer);
    }, [followLine]);
    useEffect(() => {
        if (!agentPreference.follow || !liveOn) {
            setFollowLine(undefined);
            followed.current = -1;
        }
    }, [agentPreference.follow, liveOn]);

    /*
     * FULLSCREEN: derselbe Graph, das ganze Fenster.
     *
     * Es ist der Rahmen und keine Fuehrung: die Kamera geht weiter dorthin, wo
     * der Leser sie hinschickt, und sie faehrt hier nichts von selbst ab. Escape
     * bringt das Panel zurueck, wie bei jeder anderen Flaeche, die sich ueber
     * die Oberflaeche legt, und zwar ZULETZT: liegt eine andere Flaeche darueber
     * (Hilfe, Einstiegsdialog, Suchfenster, Einstellungen), gehoert die Taste
     * ihr. Der Vollbildmodus reiht sich in die bestehende Reihenfolge ein, er
     * draengt sich nicht vor.
     */
    const escapeTaken = props.escapeTaken === true;
    useEffect(() => {
        const node = panel.current;
        if (node === null || !fullscreenIsolationRequired(fullscreen, escapeTaken)) {
            return;
        }
        return isolateFullscreenBackground(node);
    }, [fullscreen, escapeTaken]);
    useEffect(() => {
        if (!agentPreference.fullscreen || !liveOn || escapeTaken) {
            return;
        }
        const onKey = (event: globalThis.KeyboardEvent): void => {
            if (event.key !== 'Escape' || event.defaultPrevented) {
                return;
            }
            event.preventDefault();
            changeAgentPreference({ fullscreen: false });
        };
        window.addEventListener('keydown', onKey);
        return () => window.removeEventListener('keydown', onKey);
    }, [agentPreference.fullscreen, liveOn, escapeTaken, changeAgentPreference]);

    /**
     * Der Knoten, um den der Ring laeuft.
     *
     * Das Symbol vor dem Leser, wenn es im Bild vorkommt, sonst der Schritt,
     * auf dem die Fuehrung steht. In dieser Reihenfolge, weil ein Klick in das
     * Bild das Symbol wechselt, ohne die Fuehrung zu bewegen, und der Ring dann
     * dorthin gehoert, wo der Leser hingegangen ist.
     */
    const pulsedNode = useMemo(() => {
        if (mode !== 'hierarchy') {
            return undefined;
        }
        for (const candidate of [focusQualifiedName, stepQualifiedName]) {
            if (candidate !== undefined && candidate.length > 0) {
                const node = index.get(candidate);
                if (node !== undefined) {
                    return node;
                }
            }
        }
        return undefined;
    }, [mode, focusQualifiedName, stepQualifiedName, index]);

    /*
     * In der Hierarchie bleibt alles hell.
     *
     * In der Galaxie dunkelt die Szene alles ausser der Nachbarschaft ab, und
     * das ist dort richtig: eine Wolke aus fuenftausend Punkten braucht eine
     * Auswahl. Hier ist der ganze Subgraph die Antwort. Ihn bis auf zwei
     * Spalten abzudunkeln hiesse, genau die Tiefe wieder zu verstecken, die
     * dieses Bild zeigen soll: die dritte Ebene waere ein Schatten. Wo der
     * Leser steht, sagt der Ring, und der ist ein deutlicheres Zeichen als
     * "nicht abgedunkelt".
     */
    useEffect(() => {
        if (mode !== 'hierarchy' || projection === undefined) {
            return;
        }
        setHighlighted(new Set(projection.data.nodes.map((node) => node.id)));
        setNote(pulsedNode === undefined ? HIERARCHY_NO_FOCUS_NOTE : '');
    }, [mode, projection, pulsedNode]);

    // Rueck-Richtung: ein Klick in die Szene oeffnet die Datei.
    const handleNodeClick = useCallback(
        (node: GraphNode) => {
            if (picture === undefined) {
                return;
            }
            // In der Hierarchie bleibt die Kamera stehen und alles hell: sie
            // rahmt den ganzen Subgraphen, und auf eine Spalte zu zoomen waere
            // wieder die Nachbarschaftsansicht, gegen die dieses Bild gebaut
            // ist. Der Ring wandert, sobald das Symbol vor dem Leser wechselt.
            if (mode === 'galaxy') {
                const ids = neighbourIds(node.id, picture.edges);
                setHighlighted(ids);
                flyTo(picture.nodes, ids, node.qualified_name ?? node.name);
            }
            if (node.file_path === undefined || node.file_path.length === 0) {
                setNote(unopenableNodeNote(node));
                return;
            }
            setNote('');
            onOpenNode(node);
        },
        [picture, mode, flyTo, onOpenNode],
    );

    const handleBackgroundClick = useCallback(() => {
        setHighlighted(null);
        setNote(mode === 'hierarchy' ? HIERARCHY_NO_FOCUS_NOTE : GALAXY_NO_FOCUS_NOTE);
    }, [mode]);

    /*
     * Die Vorgabe der Ansicht, mit der Wahl des Lesers darauf.
     *
     * In dieser Reihenfolge und multiplikativ (siehe `displayWith` in
     * density.ts): die Hierarchie nimmt das Leuchten weg, weil es dort aus
     * sechzig Namen Flecken macht, und ein eingeschaltetes Bloom im
     * Einstellungen-Panel darf diesen Befund nicht ueberstimmen.
     */
    const choice = props.display ?? DEFAULT_GRAPH_DISPLAY;
    const display = displayWith(
        mode === 'hierarchy' ? HIERARCHY_DISPLAY : DEFAULT_DISPLAY_SETTINGS,
        choice,
    );
    const layoutState = error.length > 0 ? 'failed' : data === undefined ? 'loading' : 'ready';
    // Die Hierarchie braucht das Layout nicht: sie faerbt sich damit, sie lebt
    // nicht davon. Ein Walk, dessen Bild dasteht, ist fertig.
    const state = mode === 'hierarchy' ? 'ready' : layoutState;
    const headline =
        mode === 'hierarchy' && projection !== undefined
            ? hierarchyHeadline(projection, hierarchyOrigin)
            : layoutState === 'failed'
                ? `layout unavailable: ${error}`
                : layoutState === 'loading'
                    ? `loading the layout of ${project.length > 0 ? project : 'no project'} ...`
                    : layoutSummary(data as GraphData, LAYOUT_NODE_BUDGET);

    /*
     * Die zweite Zeile des Kopfes: woraus die Linien bestehen (W9).
     *
     * Eine eigene Zeile und kein Anhang an den Satz darueber, aus zwei
     * Gruenden. Der erste ist inhaltlich: der Satz oben sagt, WORAUF man sieht
     * (welches Symbol, wie viele, wie tief), diese Zeile sagt, WORAUS das Bild
     * besteht und was gerade fehlt. Der zweite ist handfest: der Satz oben ist
     * zitierfaehig, ein Beweislauf aus W4e liest ihn bis zu seinem Ende, und
     * eine Zeile, an die immer noch etwas angehaengt wird, ist kein Satz mehr,
     * sondern eine Sammelstelle.
     *
     * Steht nichts an (Galaxie ohne Filter), steht hier auch nichts.
     */
    const edgeNote = [
        mode === 'hierarchy' && projection !== undefined
            ? hierarchyEdgeNote(projection.data.edges.length, indexEdges.length)
            : '',
        kindNote,
    ].filter((part) => part.length > 0).join('; ');

    /*
     * Der Griff der Agentenebene.
     *
     * Er traegt genau das, was gezeichnet und geschrieben wurde, samt den
     * Winkeln aus dem zuletzt gezeichneten Bild: der Beweislauf soll die
     * Bewegung an derselben Zahl messen, die den Koerper bewegt hat, und nicht
     * an einer zweiten daneben.
     */
    useEffect(() => {
        const runtime = props.agents;
        if (runtime === undefined || agentsView === undefined) {
            globalThis.__atlasAgents = undefined;
            return;
        }
        globalThis.__atlasAgents = {
            on: runtime.on,
            layerOn: agentLayerOn,
            sourceState: runtime.status.state,
            origin: runtime.status.origin,
            requests: runtime.status.requests,
            drops: runtime.status.drops,
            mode: runtime.status.hello?.mode ?? '',
            file: runtime.status.hello?.file ?? '',
            error: runtime.status.error,
            port: runtime.port,
            events: agentsView.events,
            missed: agentsView.missed,
            perMinute: agentsView.perMinute,
            unreadable: agentsView.unreadable,
            size: agentPreference.size,
            filter: agentPreference.filter,
            follow: agentPreference.follow,
            trails: agentPreference.trails,
            fullscreen: agentPreference.fullscreen,
            trailWindowMs: agentPreference.trailWindowMs,
            shown: agentsView.actors.length,
            cap: agentsView.cap,
            capped: agentsView.capped,
            drawn: agentsView.actors.filter((actor) => actor.drawn).length,
            transitionMs: TRANSITION_MS,
            effects: {
                tails: props.agentEffects?.tails !== false,
                trails: props.agentEffects?.trails !== false,
                waves: props.agentEffects?.waves !== false,
                timeline: props.agentEffects?.timeline !== false,
            },
            timeline: timeline === undefined
                ? undefined
                : {
                    mode: timeline.mode,
                    from: timeline.from,
                    to: timeline.to,
                    windowMs: timeline.windowMs,
                    tracks: timeline.tracks.length,
                    ticks: timeline.ticks,
                    shown: timelineShown,
                    width: Math.round(sceneWidth),
                },
            follow_: followGoal.current,
            radii: Object.fromEntries(orbitRadii(
                agentsView.actors.filter((actor) => actor.drawn),
            ).entries()),
            positions: { ...agentPositions },
            pulses: { ...agentPulseScale },
            tails: { ...agentTails },
            motion: JSON.parse(JSON.stringify(agentMotion)) as Record<string, unknown>,
            camera: { ...agentCamera },
            /*
             * SceneProbe schreibt diese kleine bestehende Messnaht direkt aus
             * dem Three-Bild fort. Ein Snapshot hier waere nach seinem
             * naechsten Scan alt, obwohl genau diese Werte als "gerade
             * gezeichnet" angeboten werden. Die Referenz behaelt die
             * Produktlogik unveraendert und zeigt die aktuelle Messung.
             */
            renderOrders: agentRenderOrders,
            waves: agentsView.actors.flatMap((actor) =>
                actor.waves.map((burst) => ({
                    actor: actor.id,
                    key: burst.key,
                    nodeId: burst.nodeId,
                    events: burst.events,
                    from: burst.from,
                    to: burst.to,
                }))),
            ticker: agentsView.ticker.map((entry) => ({
                ts: entry.ts,
                actor: entry.actor,
                kind: entry.kind,
                place: entry.place,
                lines: [...entry.lines],
                text: agentText.tickerLine(
                    entry.name, WORK_KIND_WORD[entry.kind], entry.place, entry.lines,
                ),
            })),
            actors: agentsView.all.map((actor) => ({
                id: actor.id,
                name: actor.name,
                you: actor.you,
                color: actor.color,
                letter: actor.letter,
                kind: actor.kind,
                kindLetter: actor.kindLetter,
                placement: actor.placement.kind,
                uncertain: actor.placement.uncertain,
                nodeId: actor.placement.nodeId ?? -1,
                qualifiedName: actor.placement.qualifiedName,
                placeName: actor.placement.name,
                why: actor.placement.why,
                ghosts: actor.ghostNodes.map((ghost) => ghost.id),
                testedNodeId: actor.testedNode?.id ?? -1,
                intent: actor.intent,
                count: actor.count,
                missed: actor.missed,
                strip: [...actor.strip],
                paths: [...actor.paths],
                lastTool: actor.last.tool,
                lastPath: actor.last.path,
                lastLines: actor.last.lines === undefined ? [] : [...actor.last.lines],
                angle: agentAngles[actor.id] ?? -1,
                idle: actor.idle,
                sinceMs: actor.sinceMs,
                recentEvents: actor.recentEvents,
                pulseMs: actor.pulse.periodMs,
                pulseAmplitude: actor.pulse.amplitude,
                trail: actor.trail.map((node) => node.id),
                drawn: actor.drawn,
            })),
            unmapped: agentsView.unmapped.map((event) => ({ ...event })),
            angles: agentAngles,
        };
    });

    // Der Griff wird bei jedem Zustandswechsel neu gesetzt, damit er nie eine
    // Lage von vorhin beschreibt.
    useEffect(() => {
        globalThis.__atlasGalaxy = {
            nodes: data?.nodes.length ?? 0,
            targetChanges: targetChanges.current,
            highlightedCount: highlighted?.size ?? 0,
            lastTargetQn: lastTargetQn.current,
            clickNode: (qualifiedName: string): boolean => {
                const node = index.get(qualifiedName);
                if (node === undefined) {
                    return false;
                }
                handleNodeClick(node);
                return true;
            },
            legendOpen,
            legendEntries: legend.length,
            bloom: display.bloom,
            labelBoxes: labelBoxes.current,
            mode,
            open: visible,
            hierarchyAvailable: projection !== undefined,
            hierarchyOrigin: projection === undefined ? '' : hierarchyOrigin,
            fits: fitCount.current,
            lastFit: lastFit.current,
            headline,
            pulsedQn: pulsedNode?.qualified_name ?? pulsedNode?.name ?? '',
            edgeKinds: kinds.map((kind) => ({ ...kind, hidden: hiddenKinds.has(kind.type) })),
            hiddenKinds: [...hiddenKinds].sort(),
            drawnEdges: shown?.edges.length ?? 0,
            edgeNote,
            hierarchy:
                projection === undefined
                    ? undefined
                    : {
                        root: projection.rootKey,
                        rootName: projection.rootName,
                        nodes: projection.symbols,
                        depth: projection.depth,
                        truncated: projection.truncated,
                        cap: projection.cap,
                        walkDepth: projection.walkDepth,
                        placements: projection.placements.map((placement) => ({
                            key: placement.key,
                            name: placement.name,
                            file: projection.data.nodes[placement.id]?.file_path ?? '',
                            hop: placement.hop,
                            x: placement.x,
                            y: placement.y,
                        })),
                        edges: projection.data.edges.map((edge) => ({
                            from: projection.data.nodes[edge.source]?.qualified_name
                                ?? projection.data.nodes[edge.source]?.name ?? '',
                            to: projection.data.nodes[edge.target]?.qualified_name
                                ?? projection.data.nodes[edge.target]?.name ?? '',
                        })),
                        walkEdges: projection.data.edges.length,
                        extraEdges: indexEdges.length,
                        extras: indexEdges.map((edge) => ({
                            type: edge.type,
                            from: projection.data.nodes[edge.source]?.qualified_name
                                ?? projection.data.nodes[edge.source]?.name ?? '',
                            to: projection.data.nodes[edge.target]?.qualified_name
                                ?? projection.data.nodes[edge.target]?.name ?? '',
                            offset: edge.offset ?? 0,
                        })),
                    },
        };
    });

    /*
     * Der Ring um den Schritt, auf dem der Leser steht.
     *
     * Ein DOM-Element in der Szene, kein Objekt der Szene: siehe Entscheidung 9
     * im Kopf. Er zeigt und faengt nichts ab, damit ein Klick weiter den Knoten
     * darunter trifft.
     */
    const pulseRing: ReactNode =
        mode === 'hierarchy' && pulsedNode !== undefined ? (
            <Html
                position={[pulsedNode.x, pulsedNode.y, pulsedNode.z]}
                center
                style={{ pointerEvents: 'none' }}
            >
                <span
                    className="atlas-hierarchy-pulse"
                    data-testid="atlas-hierarchy-pulse"
                    data-qn={pulsedNode.qualified_name ?? pulsedNode.name}
                />
            </Html>
        ) : undefined;

    /*
     * Beide Ueberlagerungen an derselben Prop.
     *
     * Die Szene weiss weiter nicht, was sie da einhaengt (Aenderung 8 in
     * GraphScene.tsx), und sie muss zwischen einem Ring und einer Ebene aus
     * Koerpern nicht unterscheiden: was hier steht, ist ein Kind ihres Baums.
     */
    const overlay: ReactNode = (pulseRing === undefined && !liveOn) ? undefined : (
        <>
            {pulseRing}
            {liveOn && agentLayerOn && agentsView !== undefined && (
                <AgentLayer
                    actors={agentsView.actors}
                    effects={{
                        tails: props.agentEffects?.tails !== false,
                        trails: props.agentEffects?.trails !== false && agentPreference.trails,
                        waves: props.agentEffects?.waves !== false,
                    }}
                />
            )}
        </>
    );

    return (
        <section
            ref={panel}
            className="atlas-galaxy"
            data-testid="atlas-galaxy"
            data-visible={visible}
            data-state={state}
            data-mode={mode}
            data-fullscreen={fullscreen}
            data-replay={replayAt !== undefined}
        >
            <header className="atlas-galaxy-head">
                <div className="atlas-galaxy-head-row" data-hint-keep="graph head">
                    <span className="atlas-galaxy-title">GALAXY</span>
                    {/*
                      * Die beiden Ansichten sind ein Paar, und der Rahmen um sie
                      * sagt das, bevor jemand klickt.
                      *
                      * Nutzerbefund vom 2026-08-29: "galaxy Knopf macht nichts."
                      * Zwei Knoepfe nebeneinander, von denen einer wirklich
                      * etwas zuklappt und der andere ein Umschalter ist, sehen
                      * ohne Rahmen aus wie zwei Knoepfe derselben Art. Mit
                      * Rahmen, `role="group"` und `aria-pressed` ist zu sehen,
                      * dass genau EINER von ihnen aktiv ist.
                      *
                      * Seit W10b klappen sie ausserdem (Entscheidung 16), und
                      * `data-action` sagt an jedem Chip, was ein Klick JETZT
                      * tut: `collapse`, `open` oder `switch`. Nicht `data-fold`:
                      * die Marke gehoert den Schaltern, die nichts anderes tun
                      * als auf- und zuklappen (tools/smoke-w8b.mjs liest sie so),
                      * und diese hier tun je nach Lage zweierlei.
                      *
                      * `aria-pressed` bleibt dabei die GEWAEHLTE Ansicht und
                      * nicht die sichtbare: die Wahl ueberlebt das Zuklappen und
                      * ist es, was beim Aufklappen wieder dasteht. Ob die
                      * Sektion offen ist, sagt `data-open` am Rahmen und der
                      * beschriftete Schalter daneben in Worten.
                      */}
                    <div
                        className="atlas-graph-mode"
                        data-testid="atlas-graph-mode"
                        data-mode={mode}
                        data-open={visible}
                        role="group"
                        aria-label="which picture the panel shows"
                    >
                        {GRAPH_MODES.map((candidate) => {
                            const available = candidate === 'galaxy' || projection !== undefined;
                            const action = !available
                                ? 'none'
                                : !visible
                                    ? 'open'
                                    : mode === candidate ? 'collapse' : 'switch';
                            return (
                                <Hint
                                    key={candidate}
                                    name={`graph-mode-${candidate}`}
                                    text={
                                        !available
                                            ? HIERARCHY_UNAVAILABLE_TITLE
                                            : !visible
                                                ? graphModeCollapsedTitle(candidate)
                                                : mode === candidate
                                                    ? graphModeActiveTitle(candidate)
                                                    : candidate === 'galaxy'
                                                        ? 'galaxy: the whole project, laid out by the server'
                                                        : 'hierarchy: what the chosen symbol reaches, one column per call depth'
                                    }
                                >
                                    <button
                                        type="button"
                                        className="atlas-graph-mode-chip"
                                        data-testid="atlas-graph-mode-chip"
                                        data-mode={candidate}
                                        data-active={mode === candidate}
                                        data-action={action}
                                        data-available={available}
                                        aria-pressed={mode === candidate}
                                        /*
                                         * `aria-disabled` und nicht `disabled`:
                                         * ein Knopf, den der Browser sperrt,
                                         * bekommt keine Zeigerereignisse mehr,
                                         * also oeffnet auch sein Tooltip nicht.
                                         * Er waere stumm, und AC3 verlangt das
                                         * Gegenteil: deaktiviert UND sagt warum.
                                         * Der Klick antwortet deshalb mit
                                         * demselben Satz in der Notizzeile.
                                         */
                                        aria-disabled={!available}
                                        onClick={() => {
                                            if (!available) {
                                                setNote(HIERARCHY_UNAVAILABLE_TITLE);
                                                return;
                                            }
                                            if (visible && mode === candidate) {
                                                props.onToggleVisible?.();
                                                return;
                                            }
                                            setChosenMode(candidate);
                                            if (!visible) {
                                                props.onToggleVisible?.();
                                            }
                                        }}
                                    >
                                        {candidate}
                                    </button>
                                </Hint>
                            );
                        })}
                    </div>
                    <Hint
                        name="galaxy-legend"
                        text={
                            legendOpen
                                ? 'hide the legend: what the colours, sizes and positions mean'
                                : 'show the legend: what the colours, sizes and positions mean'
                        }
                    >
                        <button
                            type="button"
                            className="atlas-galaxy-legend-toggle"
                            data-testid="atlas-galaxy-legend-toggle"
                            aria-expanded={legendOpen}
                            aria-controls="atlas-galaxy-legend"
                            data-fold={legendOpen ? 'collapse' : 'open'}
                            data-fold-of="legend"
                            onClick={toggleLegend}
                        >
                            {legendFoldLabel(legendOpen)}
                        </button>
                    </Hint>
                    {/*
                      * Der Zuklapp-Schalter steht IM Kopf des Panels.
                      *
                      * Nutzerfeedback vom 2026-08-29: zugeklappt fiel das ganze
                      * Panel auf Hoehe null, und der einzige Weg zurueck war ein
                      * Menuepunkt, den man kennen musste. Der Kopf bleibt jetzt
                      * stehen, und der Schalter darin sagt in beiden Lagen, was
                      * ein Klick tut: seit W8b in Worten und mit dem Namen des
                      * Bildes, das gerade dasteht. Warum, steht an
                      * {@link graphFoldLabel}.
                      */}
                    {props.onToggleVisible !== undefined && (
                        <Hint
                            name="galaxy-collapse"
                            text={visible ? GALAXY_COLLAPSE_TITLE : GALAXY_EXPAND_TITLE}
                        >
                            <button
                                type="button"
                                className="atlas-galaxy-collapse"
                                data-testid="atlas-galaxy-collapse"
                                aria-expanded={visible}
                                data-fold={visible ? 'collapse' : 'open'}
                                data-fold-of={mode}
                                onClick={props.onToggleVisible}
                            >
                                {graphFoldLabel(visible, mode)}
                            </button>
                        </Hint>
                    )}
                </div>
                <span
                    className="atlas-galaxy-headline"
                    data-testid="atlas-galaxy-headline"
                    data-state={state}
                >
                    {headline}
                </span>
                {edgeNote.length > 0 && (
                    <span
                        className="atlas-galaxy-edgenote"
                        data-testid="atlas-galaxy-edgenote"
                        data-hidden-kinds={hiddenHere}
                        data-kinds={kinds.length}
                    >
                        {edgeNote}
                    </span>
                )}
            </header>
            {legendOpen && (
                /*
                 * Der Rahmen um die Legende traegt ihre Kante.
                 *
                 * Der Hinweis steht NEBEN dem scrollenden Kasten und nicht
                 * darin: was im Kasten steht, scrollt mit und waere genau dann
                 * weg, wenn er gebraucht wird. Er liegt darum absolut im
                 * Rahmen, faengt keine Klicks ab und deckt keinen Text zu; der
                 * Verlauf darunter loest die letzte Zeile auf, statt sie
                 * abzuschneiden, damit ein halber Satz an der Kante als
                 * Fortsetzung zu lesen ist und nicht als Fehler.
                 */
                <div className="atlas-galaxy-legend-frame" data-testid="atlas-galaxy-legend-frame">
                    <div
                        className="atlas-galaxy-legend"
                        id="atlas-galaxy-legend"
                        data-testid="atlas-galaxy-legend"
                        data-more-above={legendEdge.above}
                        data-more-below={legendEdge.below}
                        ref={legendBox}
                        onScroll={measureLegendEdge}
                    >
                        {legend.map((entry) => (
                            <p
                                className="atlas-galaxy-legend-entry"
                                data-testid="atlas-galaxy-legend-entry"
                                data-entry={entry.key}
                                key={entry.key}
                            >
                                <b>{entry.title}</b>
                                {entry.swatches.length > 0 && (
                                    <span className="atlas-galaxy-legend-swatches">
                                        {entry.swatches.map((swatch) => {
                                            const hidden = hiddenKinds.has(swatch.label);
                                            const dot = (
                                                <span
                                                    className="atlas-galaxy-legend-dot"
                                                    style={{ background: swatch.color }}
                                                    aria-hidden="true"
                                                />
                                            );
                                            const label = swatch.count === undefined
                                                ? swatch.label
                                                : `${swatch.label} ${swatch.count}`;
                                            /*
                                             * Ein Schalter nur dort, wo es etwas zu
                                             * schalten gibt: der graue Punkt der
                                             * Hierarchie steht fuer eine
                                             * Abwesenheit, und ein Knopf, der sie
                                             * ausblendet, waere ein Knopf ohne
                                             * Wirkung.
                                             */
                                            return entry.filterable === true ? (
                                                <Hint
                                                    key={swatch.label}
                                                    name={`legend-${swatch.label}`}
                                                    text={
                                                        hidden
                                                            ? `${swatch.label} is hidden: click to draw these edges again`
                                                            : `hide the ${swatch.label} edges; the kind stays here, dimmed`
                                                    }
                                                >
                                                    <button
                                                        type="button"
                                                        className="atlas-galaxy-legend-swatch"
                                                        data-testid="atlas-galaxy-legend-swatch"
                                                        data-type={swatch.label}
                                                        data-color={swatch.color}
                                                        data-count={swatch.count}
                                                        data-hidden={hidden}
                                                        aria-pressed={!hidden}
                                                        onClick={() => toggleKind(swatch.label)}
                                                    >
                                                        {dot}
                                                        {label}
                                                    </button>
                                                </Hint>
                                            ) : (
                                                <span
                                                    className="atlas-galaxy-legend-swatch"
                                                    data-testid="atlas-galaxy-legend-swatch"
                                                    data-type={swatch.label}
                                                    data-color={swatch.color}
                                                    key={swatch.label}
                                                >
                                                    {dot}
                                                    {label}
                                                </span>
                                            );
                                        })}
                                    </span>
                                )}
                                <span className="atlas-galaxy-legend-detail">{entry.detail}</span>
                            </p>
                        ))}
                    </div>
                    {(legendEdge.above || legendEdge.below) && (
                        <span
                            className="atlas-galaxy-legend-more"
                            data-testid="atlas-galaxy-legend-more"
                            data-scroll-hint={[
                                legendEdge.above ? 'top' : '',
                                legendEdge.below ? 'bottom' : '',
                            ].filter((part) => part.length > 0).join(' ')}
                            data-edge={legendEdge.below ? 'bottom' : 'top'}
                        >
                            {legendEdge.below ? '▾ more' : '▴ more'}
                        </span>
                    )}
                </div>
            )}
            <div className="atlas-galaxy-scene" data-testid="atlas-galaxy-scene" ref={scene}>
                {/*
                  * Der Weg zurueck zur eingepassten Ansicht (AC5).
                  *
                  * Er steht IN der Szene und nicht im Kopf, und das ist gemessen
                  * und nicht Geschmack: die Kopfzeile ist bei 440 Pixeln Breite
                  * mit Marke, Ansichts-Schalter, Legenden-Schalter und
                  * Zuklapper schon voll, und ein fuenftes Wort darin haette den
                  * Zuklapper an der Kante abgeschnitten. Hier liegt er ausserdem
                  * dort, wo der Leser gerade zieht und dreht. Dieselbe Bauform
                  * wie das Instrument der Agentenebene, nur in der anderen Ecke.
                  */}
                {visible && (
                    <Hint name="galaxy-fit" text={GALAXY_FIT_TITLE}>
                        <button
                            type="button"
                            className="atlas-galaxy-fit"
                            data-testid="atlas-galaxy-fit"
                            data-fits={fitCount.current}
                            onClick={refitNow}
                        >
                            {GALAXY_FIT_LABEL}
                        </button>
                    </Hint>
                )}
                {shown !== undefined && everVisible.current && (
                    <GraphScene
                        active={visible}
                        data={shown}
                        display={display}
                        highlightedIds={highlighted}
                        cameraTarget={cameraTarget}
                        /*
                         * In der Hierarchie tragen alle Namen dieselbe
                         * Weltgroesse und eine Breitengrenze, die das
                         * Spaltenraster einhaelt. Warum, steht an den
                         * Konstanten in hierarchy-layout.ts. In der Galaxie
                         * bleibt es bei der Rechnung der Uebernahme.
                         */
                        labelWorldFontSize={
                            mode === 'hierarchy' ? HIERARCHY_LABEL_FONT_SIZE : undefined
                        }
                        labelMaxTextWidth={
                            mode === 'hierarchy' ? HIERARCHY_LABEL_MAX_TEXT_WIDTH : undefined
                        }
                        onLabelLayout={onLabelLayout}
                        /*
                         * Namen erst, wenn etwas im Fokus steht.
                         *
                         * Die Szene beschriftet die achtzig groessten Knoten,
                         * und in einem Panel dieser Breite sind achtzig
                         * Namen bei Uebersichtsabstand kein Text, sondern
                         * Rauschen: sie ueberlagern sich zu Flecken, die wie
                         * ein Rendering-Fehler aussehen. Steht ein Symbol im
                         * Fokus, beschriftet dieselbe Szene nur noch dessen
                         * Nachbarschaft, und dann ist jeder Name lesbar und
                         * jeder Name eine Antwort auf die Frage, die gerade
                         * gestellt wurde.
                         *
                         * In der Hierarchie sind die Namen immer an: dort
                         * stehen hoechstens sechzig Punkte, und eine
                         * Aufrufkette ohne Namen waere eine Reihe Punkte.
                         */
                        showLabels={
                            mode === 'hierarchy'
                                ? shown.nodes.length <= HIERARCHY_LABEL_BUDGET
                                : highlighted !== null && highlighted.size > 0
                        }
                        /*
                         * Landmarken nur in der Galaxie: der Halo sitzt auf den
                         * groessten Knoten, und "gross" heisst in der Projektion
                         * mal "viele Kanten im ganzen Graphen" und mal "keine
                         * Layout-Angabe". Ein Leuchten darauf waere eine
                         * Behauptung, die die halbe Zeit nichts bedeutet.
                         */
                        landmarks={mode === 'galaxy' && choice.halos}
                        /*
                         * Die vier Einstellungen aus dem Panel (W10). Sie gehen
                         * unveraendert durch: was sie bedeuten, steht in
                         * density.ts, und was sie in der Szene tun, in
                         * GraphScene.tsx.
                         */
                        projection={choice.projection}
                        drawEdges={choice.edges !== 'off'}
                        labelDistanceFactor={choice.labelDistanceFactor}
                        frameCap={choice.frameCap}
                        onNodeClick={handleNodeClick}
                        onBackgroundClick={handleBackgroundClick}
                        renderTooltip={(node) => <NodeTooltipCard node={node} />}
                        overlay={overlay}
                    />
                )}
                {state !== 'ready' && (
                    <p className="atlas-galaxy-placeholder" data-state={state}>
                        {headline}
                    </p>
                )}
                {/*
                  * Das Instrument liegt IM Kasten der Szene und nicht darunter:
                  * es erklaert, was auf dem Graphen zu sehen ist, und ein Kasten
                  * daneben waere eine zweite Flaeche, die man zwischen Bild und
                  * Text hin und her lesen muesste. Es ist halbtransparent und
                  * faengt seine eigenen Klicks ab; der Rest der Flaeche bleibt
                  * die Szene.
                  */}
                {liveOn && agentsView !== undefined && props.agents !== undefined && (
                    <AgentsHud
                        view={agentsView}
                        status={props.agents.status}
                        port={props.agents.port}
                        size={agentPreference.size}
                        onSize={(size: HudSize) => changeAgentPreference({ size })}
                        filter={agentPreference.filter}
                        onFilter={(filter: ActorFilter) => changeAgentPreference({ filter })}
                        switches={{
                            follow: agentPreference.follow,
                            trails: agentPreference.trails,
                            fullscreen: agentPreference.fullscreen,
                        }}
                        onSwitch={(name: keyof HudSwitches) =>
                            changeAgentPreference({ [name]: !agentPreference[name] })}
                        trailWindowMs={agentPreference.trailWindowMs}
                        onTrailWindow={(trailWindowMs: number) =>
                            changeAgentPreference({ trailWindowMs })}
                        layerOn={agentLayerOn}
                        column={fullscreen}
                    />
                )}

                {/*
                  * Der Streifen unten: die Ereigniszeile des FOLLOW-Modus und
                  * darunter der Zeitstrahl.
                  *
                  * Beide in EINEM Stapel und nicht zwei Mal absolut positioniert:
                  * sie stehen beide unten, ihre Hoehen haengen an der Zahl der
                  * Akteure, und zwei Kaesten, die sich mit wachsender Zahl
                  * ineinander schieben, waeren eine Ueberlagerung, die erst beim
                  * neunten Agenten auffaellt. Der Stapel endet vor dem
                  * Instrument, damit auch dort nichts uebereinander liegt.
                  *
                  * Er erscheint erst ab {@link TIMELINE_MIN_WIDTH} Pixeln
                  * Zeichenflaeche, und die Grenze ist gemessen: rechts steht das
                  * Instrument, und was links davon uebrig bleibt, traegt in
                  * einem Panel von 441 Pixeln weder eine lesbare Zeile noch eine
                  * Spur, auf der ein Strich vom naechsten zu unterscheiden ist.
                  */}
                {liveOn && sceneWidth >= TIMELINE_MIN_WIDTH && (
                    <div className="atlas-galaxy-bottom" data-testid="atlas-galaxy-bottom">
                        {agentPreference.follow && followLine !== undefined && (
                            <Hint name="agents-followline" text={agentText.followLineTitle}>
                                <p
                                    className="atlas-agents-followline"
                                    data-testid="atlas-agents-followline"
                                    data-actor={followLine.id}
                                    data-kind={followLine.kind}
                                    data-place={followLine.placement.name}
                                    data-lines={(followLine.last.lines ?? []).join('-')}
                                    style={{ ['--atlas-agent-color' as string]: followLine.color }}
                                >
                                    {agentText.followLine(
                                        followLine.name,
                                        WORK_KIND_WORD[followLine.kind],
                                        followLine.placement.name,
                                        followLine.last.lines ?? [],
                                    )}
                                </p>
                            </Hint>
                        )}
                        {timelineShown && timeline !== undefined && (
                            <AgentsTimeline
                                timeline={timeline}
                                now={agentNow}
                                windowMs={agentPreference.trailWindowMs}
                                onWindow={(trailWindowMs: number) =>
                                    changeAgentPreference({ trailWindowMs })}
                                onPause={() =>
                                    setPausedAt((current) =>
                                        (current === undefined ? Date.now() : undefined))}
                                onScrub={(ts: number) => setReplayAt(ts)}
                                onLive={() => {
                                    setReplayAt(undefined);
                                    setPausedAt(undefined);
                                }}
                            />
                        )}
                    </div>
                )}
            </div>
            {note.length > 0 && (
                <p className="atlas-galaxy-note" data-testid="atlas-galaxy-note">
                    {note}
                </p>
            )}
        </section>
    );
}
