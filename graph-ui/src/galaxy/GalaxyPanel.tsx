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
import { GALAXY_NODE_LIMITS, GALAXY_EDGE_LIMITS, useViewPreferences } from '../settings/view-preferences';
import { Html } from '@react-three/drei';

import { GraphScene, computeCameraTarget, computeFitTarget, computeFrameTarget } from './GraphScene';
import type { CameraTarget } from './GraphScene';
import type { LabelBox } from './NodeLabels';
import { NodeTooltipCard } from './NodeTooltipCard';
import { HoverCardHtml } from './hover-layer';
import GalaxyNavigator from './GalaxyNavigator';
import { TraceEdgeFilter } from './TraceEdgeFilter';
import { PathPicker, PathSteps } from './ScopePathControls';
import { callOrder, pathNodes, shortestScopePath, type ScopePathStep } from './scope-path';
import { galaxyHierarchyNoteText, galaxyHierarchyText, galaxyHistoryText, galaxyLayerText, galaxyPathText, galaxyToolbarText } from './galaxy-strings';
import { graphNodeName, nodeFilePath, scopeDisplayName, scopeTitle } from './node-names';
import { HierarchyBandLabel, HierarchyEdgeLabels } from './HierarchyEdgeLabels';
import { FitLabel, useToolbarFit } from './toolbar-fit';
import { emptyNavigationHistory, moveNavigation, peekNavigation, pushNavigation } from '../graph/navigation-history';
import { useDismissibleMenu } from '../graph/use-dismissible-menu';
import { galaxyHistoryOptions, historyEntryDetail, historyEntryLabel, scopeIdentity, type GalaxyHistoryEntry, type ScopeTrail } from './scope-history';
import { useOrganicLayout } from './use-organic-layout';
import RenderProgress from './RenderProgress';
import { useGraphScope } from './use-graph-scope';
import { SCOPED_HIERARCHY_LABEL_BUDGET, SCOPED_HIERARCHY_LABEL_MAX_TEXT_WIDTH, expandPastLimit, hierarchyLabelWidth, limitGraphRender, nextLayerEstimate, scenePictureFor, scopedHierarchy, type ExpandOutlook } from './graph-scope';
import './graph-exploration.css';
import { layoutNodeForSelection } from './selected-node';
import { galaxyScopeEvidence, useSelectionEvidence, type SelectionEvidenceListener } from './selection-evidence';
import { buildCoverageShadow } from './coverage-shadow';
import type { CoverageShadowNode } from './coverage-shadow';
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
import type { HierarchyStatus } from './selection-hierarchy';
import { readerFocusFrame, readerGraphFocus, type SourceFocusRange } from './reader-graph-focus';
import { projectReaderHierarchy } from './reader-hierarchy';
import Hint from '../ui/tooltip/Hint';
import type { GraphData, GraphNode } from './types';
import type { SelectionScope } from '../why/SelectionContext';
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
import { isTypingTarget } from '../app/keyboard';

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

/*
 * Was der `hierarchy`-Chip sagt, solange es weder Walk noch Fokus gibt, und
 * was das Panel sagt, wenn kein Knoten des Walks im Reader offen ist, stehen
 * seit Runde 4 (N2) in galaxy-strings.ts (`galaxyHierarchyNoteText`). Seit
 * W10b nennt der Chip BEIDE Wege (AC3): ein offenes Symbol genuegt, ein
 * gewaehlter Start ist der andere Weg; im Galaxy-Tab ist es ein gewaehlter
 * Knoten.
 */

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
    placements: { key: string; name: string; file: string; hop: number; x: number; y: number; side?: -1 | 0 | 1; mixed?: boolean }[];
    /** Zweites Review zu K5: das Band der gemischt erreichten Knoten und wer Namen traegt. */
    band?: { count: number; x: number; y: number; left: number; right: number; bottom: number };
    names?: 'all' | 'neighbours' | 'none';
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
        /** Worauf die Kamera sieht: in einem Scope die Wurzel. */
        center?: [number, number, number];
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
    /** Der Verlauf aus Zurueck und Vor (K2): Eintraege als Tooltip-Text, der Zeiger, die letzten Wurzeln. */
    history: { index: number; entries: string[]; recent: string[] };
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
    /** Fill the workspace while leaving navigation and local chat interactive. */
    workspaceExpanded?: boolean;
    /** Select graph evidence, including nodes without a source file. */
    onSelectNode?: ((node: GraphNode) => void) | undefined;
    onSelectShadowNode?: ((node: CoverageShadowNode) => void) | undefined;
    selectedNode?: GraphNode | undefined;
    onClearSelection?: () => void;
    onSelectionEvidence?: SelectionEvidenceListener;
    /** Das Projekt, dessen Layout gezeigt wird. Leer heisst: nichts laden. */
    project: string;
    /** Ob das Panel im Layout sichtbar ist. */
    visible: boolean;
    /** Der qualifizierte Name des Twin-Subjekts, dem die Kamera folgt. */
    focusQualifiedName?: string | undefined;
    /** Der Anzeigename desselben Subjekts, fuer die ehrliche Fehlanzeige. */
    focusName?: string | undefined;
    focusFilePath?: string;
    focusSourceRange?: SourceFocusRange;
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
    focusWalkStatus?: HierarchyStatus;
    focusWalkMessage?: string;
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
    /**
     * "Selection details". As a function it receives the loaded scope (hand
     * test K13): the relationships of the selection then come from what this
     * panel loaded, not from the capped repository snapshot.
     */
    selectionPanel?: ReactNode | ((scope: SelectionScope | undefined) => ReactNode);
}

/**
 * Bis zu wie vielen Knoten ein Ausschnitt auf dem Schirm auseinandergeschoben
 * wird (Handtest K8). Die Trennung hebt Namen und Ringe kleiner Ausschnitte aus
 * der Wolke; eine dichte Wolke aus tausenden Knoten (die dritte Ebene um
 * JSONBAgg) schob sie dagegen zu einem Kreuz aus langen Linien auseinander.
 */
export const SCOPE_SEPARATION_LIMIT = 1500;

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

    const [layout, setData] = useState<GraphData | undefined>(undefined);
    const [layoutLoading, setLayoutLoading] = useState(false);
    const [spacingBusy, setSpacingBusy] = useState(false);
    const { preferences: viewPreferences, setPreferences: setViewPreferences } = useViewPreferences(project);
    const { galaxyNodes: nodeBudget, galaxyEdges: edgeBudget, coverageShadow: showCoverage } = viewPreferences;
    const [traceFilter, setTraceFilter] = useState<{ project: string; types?: string[] }>({ project });
    const traceTypes = props.workspaceExpanded && traceFilter.project === project ? traceFilter.types : undefined;
    const changeTraceTypes = useCallback((types: string[] | undefined) => setTraceFilter({ project, types }), [project]);
    const scope = useGraphScope({ project, layout,
        filePath: props.workspaceExpanded ? undefined : props.focusFilePath,
        range: props.focusSourceRange, fetch: props.fetch, edgeTypes: traceTypes,
        // Handtest K8: eine Ebene ueber dem Render-Limit haelt dort an, statt minutenlang weiterzuladen.
        ...(props.workspaceExpanded ? { limits: { nodes: nodeBudget, edges: edgeBudget } } : {}) });
    const organicHistory = useRef<{ key: string; depth: number; data: GraphData } | undefined>(undefined);
    const organicKey = JSON.stringify([project, scope.scope, scope.direction, traceTypes]);
    const organicOptions = useMemo(() => {
        const previous = organicHistory.current;
        return { rootIds: scope.result?.roots,
            previous: previous?.key === organicKey && previous.depth <= scope.depth ? previous.data : undefined };
    }, [scope.result, scope.scope, organicKey, scope.depth]);
    const organicTask = useOrganicLayout(scope.scope ? scope.result?.data : undefined, organicOptions);
    const organic = organicTask.result;
    useEffect(() => {
        if (scope.complete && organic) organicHistory.current = { key: organicKey, depth: scope.depth, data: organic.data };
        if (!scope.scope) organicHistory.current = undefined;
    }, [scope.complete, scope.scope, organic, organicKey, scope.depth]);
    const data = scope.scope ? organic?.data : layout;
    /*
     * Was die Szene zeigt, solange das naechste Bild noch angeordnet wird.
     *
     * Bis hierher war `data` waehrend jeder Anordnung leer, die Szene wurde
     * ausgehaengt, und jeder Schritt (Auswahl, Expand, Vorschau zu Antwort)
     * baute den Canvas neu auf (Review-Befund G1). Jetzt bleibt das letzte Bild
     * stehen, bis das neue fertig ist; vor dem ersten Bild eines Scopes ist das
     * der ganze Graph, der gerade noch zu sehen war. `data` bleibt dabei das
     * AKTUELLE Bild: Einpassung, Legende und Zaehler rechnen nie mit dem alten.
     */
    const sceneData = scope.scope ? scenePictureFor(organic?.data, organicTask.stale?.data, layout) : layout;
    const sceneScoped = Boolean(scope.scope) && sceneData !== layout;
    const scopedRoot = scope.result?.roots.size === 1
        ? scope.result.data.nodes.find(node => scope.result!.roots.has(node.id)) : undefined;
    const notifiedScopedSymbol = useRef('');
    useEffect(() => {
        if (scope.scope?.kind !== 'symbol') { notifiedScopedSymbol.current = ''; return; }
        if (!scope.complete || !scopedRoot) return;
        const identity = `${project}:${scope.scope.qualifiedName}`;
        if (notifiedScopedSymbol.current === identity) return;
        notifiedScopedSymbol.current = identity;
        props.onSelectNode?.(scopedRoot);
    }, [scope.scope, scope.complete, scopedRoot, project, props.onSelectNode]);
    const [error, setError] = useState('');
    useEffect(() => { if (organicTask.error) setError(organicTask.error); }, [organicTask.error]);
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
            setLayoutLoading(false);
            return;
        }
        let cancelled = false;
        setError('');
        setLayoutLoading(true);
        loadLayout(project, { ...(fetchImpl === undefined ? {} : { fetch: fetchImpl }), maxNodes: props.workspaceExpanded ? nodeBudget : LAYOUT_NODE_BUDGET })
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
            }).finally(() => { if (!cancelled) setLayoutLoading(false); });
        return () => {
            cancelled = true;
        };
    }, [project, fetchImpl, onLayout, nodeBudget, props.workspaceExpanded]);

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
    const readerHierarchyActive = !props.workspaceExpanded && walk === undefined && Boolean(props.focusFilePath);
    const readerProjection = useMemo(() => readerHierarchyActive && data
        ? projectReaderHierarchy(data, props.focusFilePath!, props.focusSourceRange, Number.MAX_SAFE_INTEGER, scope.depth > 1) : undefined,
    [readerHierarchyActive, data, props.focusFilePath, props.focusSourceRange, scope.depth]);
    const hierarchyOrigin: HierarchyRootOrigin = walk !== undefined ? 'walk'
        : readerHierarchyActive ? 'file' : 'focus';

    const scopedProjection = useMemo(() => props.workspaceExpanded && scope.scope && scope.result
        ? scopedHierarchy(scope.result, scope.scope.name) : undefined, [props.workspaceExpanded, scope.scope, scope.result]);
    /*
     * Review zu K5: bis 150 Knoten stehen in der Hierarchie eines Ausschnitts
     * alle Namen. Darueber behalten die Wurzel und ihre direkten Nachbarn ihre
     * Namen und Kantenschilder (zweites Review), und erst eine Wurzel mit mehr
     * Nachbarn als das hat keine; eine Notiz im Bild sagt es.
     */
    const scopedNames = scopedProjection?.names ?? 'all';
    const scopedNeighbours = useMemo(() => scopedProjection?.placements.filter(placement => placement.hop === 1).length ?? 0, [scopedProjection]);
    const scopedNamedIds = useMemo(() => scopedProjection?.namedIds ? new Set(scopedProjection.namedIds) : undefined, [scopedProjection]);
    const scopedNamedSides = useMemo(() => {
        const sides = { root: false, incoming: 0, outgoing: 0 };
        for (const placement of scopedProjection?.placements ?? []) {
            if (!scopedNamedIds?.has(placement.id)) continue;
            if (placement.hop === 0) sides.root = true;
            else if (placement.hop === 1 && placement.side === -1) sides.incoming += 1;
            else if (placement.hop === 1 && placement.side === 1) sides.outgoing += 1;
        }
        return sides;
    }, [scopedProjection, scopedNamedIds]);
    const scopedSpots = useMemo(() => scopedProjection
        ? new Map(scopedProjection.placements.map(placement => [placement.id, { hop: placement.hop, x: placement.x, y: placement.y }])) : undefined,
    [scopedProjection]);
    const projection = useMemo(
        () => scopedProjection ?? (readerHierarchyActive ? readerProjection
            : activeWalk === undefined ? undefined : projectHierarchy(activeWalk, { layout: data })),
        [scopedProjection, readerHierarchyActive, readerProjection, activeWalk, data],
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
    const mode: GraphMode = chosenMode ?? (walk === undefined ? 'galaxy' : 'hierarchy');

    /*
     * Was der Index ausser den Aufrufen zwischen den gezeigten Symbolen kennt.
     *
     * Haengt an der Projektion UND am geladenen Layout, weil es von dort kommt.
     * Ohne Layout ist die Liste leer, und die Hierarchie zeigt genau das, was
     * sie vor W9 gezeigt hat.
     */
    const indexEdges = useMemo(
        () => (readerHierarchyActive || scopedProjection || projection === undefined ? [] : hierarchyIndexEdges(projection, data)),
        [readerHierarchyActive, scopedProjection, projection, data],
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
        if (mode !== 'hierarchy') {
            return data;
        }
        if (projection === undefined) return undefined;
        return indexEdges.length === 0
            ? projection.data
            : { ...projection.data, edges: [...projection.data.edges, ...indexEdges] };
    }, [mode, projection, indexEdges, data]);

    const kinds = useMemo(() => edgeKinds(picture), [picture]);
    const hiddenHere = kinds.filter((kind) => hiddenKinds.has(kind.type)).length;
    const kindNote = edgeKindNote(kinds.length, hiddenHere);

    /*
     * Die Szene bekommt in der Galaxie das stehende Bild (siehe `sceneData`).
     * Steht dort noch der ganze Graph, gelten auch seine ausgeblendeten
     * Kantenarten weiter; erst ein Scope-Bild filtert ueber `traceTypes`.
     * `shown` ist nur das AKTUELLE Bild: Kantenfilter, Zaehler und Auskunft an
     * den Chat rechnen nie mit dem Platzhalter.
     */
    const scenePicture = mode === 'hierarchy' ? picture : sceneData;
    const sceneShown = useMemo(
        () => {
            if (!scenePicture) return undefined;
            const traced = props.workspaceExpanded && scope.scope && (mode === 'hierarchy' || sceneScoped);
            const filtered = traced ? scenePicture : withoutEdgeKinds(scenePicture, hiddenKinds);
            const requiredNames = new Set(data?.nodes.filter(node => scope.result?.roots.has(node.id)).map(node => node.qualified_name));
            return props.workspaceExpanded ? limitGraphRender(filtered, nodeBudget, edgeBudget, mode === 'hierarchy'
                ? new Set(filtered.nodes.filter(node => requiredNames.has(node.qualified_name)).map(node => node.id))
                : scope.result?.roots) : filtered;
        },
        [scenePicture, hiddenKinds, props.workspaceExpanded, nodeBudget, edgeBudget, scope.result?.roots, scope.scope, mode, data, sceneScoped],
    );
    const shown = mode === 'hierarchy' || sceneData === data ? sceneShown : undefined;

    const traceKinds = useMemo(() => edgeKinds(shown), [shown]);
    const agentEvidence = useMemo(() => scope.scope ? galaxyScopeEvidence({ project, identity: scope.scope,
        nodes: scope.result?.data.nodes ?? [], edges: scope.result?.data.edges ?? [], roots: scope.result?.roots ?? new Set<number>(),
        depth: scope.depth, direction: props.workspaceExpanded ? scope.direction : 'both', edgeTypes: traceTypes ?? 'all',
        // A layer that stopped at the render limit is loaded, not complete: the chat says so (C1).
        state: scope.complete ? scope.result?.partial ? 'render-limit-partial' : 'complete-indexed-scope' : scope.loading ? 'loading-partial-preview' : 'partial',
        error: scope.error, exhausted: scope.result?.exhausted,
        ...scope.complete && scope.result?.partial ? { renderLimit: { layer: scope.result.partial.layer, kind: scope.result.partial.limit,
            limit: scope.result.partial.limit === 'nodes' ? nodeBudget : edgeBudget } } : {},
        // Which picture is shown, so the chat can say how to read it (H1).
        display: mode }) : undefined,
    [project, scope.scope, scope.result, scope.depth, scope.direction, scope.complete, scope.loading, scope.error, traceTypes, props.workspaceExpanded, nodeBudget, edgeBudget, mode]);
    useSelectionEvidence(props.onSelectionEvidence, agentEvidence, visible && props.workspaceExpanded === true);
    const toggleKind = useCallback((type: string) => {
        if (props.workspaceExpanded && scope.scope) {
            const next = new Set(traceTypes ?? kinds.map(kind => kind.type));
            if (!next.delete(type)) next.add(type);
            changeTraceTypes([...next].sort());
            return;
        }
        setHiddenKinds((hidden) => {
            const next = new Set(hidden);
            if (!next.delete(type)) next.add(type);
            return next;
        });
    }, [props.workspaceExpanded, scope.scope, traceTypes, kinds, changeTraceTypes]);

    const renderLimits = <>
        <label>Nodes <select aria-label="Rendered node limit" value={nodeBudget} onChange={event => setViewPreferences({ galaxyNodes: Number(event.target.value) })}>
            {GALAXY_NODE_LIMITS.map(value => <option key={value} value={value}>{value.toLocaleString()}</option>)}
        </select></label>
        <label>Edges <select aria-label="Rendered edge limit" value={edgeBudget} onChange={event => setViewPreferences({ galaxyEdges: Number(event.target.value) })}>
            {GALAXY_EDGE_LIMITS.map(value => <option key={value} value={value}>{value.toLocaleString()}</option>)}
        </select></label>
    </>;

    /*
     * Der Pfad und die Aufrufreihe (Review-Befund G5, nach dem Vorbild von
     * Graphify).
     *
     * Beide rechnen nur auf dem, was dieser Scope schon geladen hat, und in der
     * Richtung, in der er verfolgt wird: der Server wird nicht gefragt, und ein
     * fehlender Pfad heisst "nicht in diesen Beziehungen", nicht "gibt es
     * nicht". Der Abstand ist hier die Zahl der Hops und nur hier (G6); die
     * Wolke selbst behauptet keinen. Die Wahl haengt am Scope (`organicKey`):
     * eine neue Wurzel, Richtung oder Kantenart verwirft sie, ein Expand
     * rechnet sie auf dem groesseren Bild neu. Verworfen heisst verworfen: wer
     * die Richtung zurueckstellt oder dieselbe Wurzel neu waehlt, bekommt den
     * alten Pfad nicht ungefragt wieder. Das Ziel behaelt seinen Namen, auch
     * wenn ein entfernter Layer es aus dem Bild nimmt.
     */
    const [trail, setTrail] = useState<{ key: string; kind: 'path'; target: number; name: string } | { key: string; kind: 'calls' }>();
    const [trailStep, setTrailStep] = useState(0);
    useEffect(() => {
        setTrail(current => (current && current.key !== organicKey ? undefined : current));
        setTrailStep(0);
    }, [organicKey]);
    const trailRoot = scope.result?.roots.size === 1 ? [...scope.result.roots][0] : undefined;
    const rootCalls = useMemo(() => (data && trailRoot !== undefined ? callOrder(data.edges, trailRoot) : []), [data, trailRoot]);
    const pathCandidates = useMemo(() => data?.nodes.filter(node => !scope.result?.roots.has(node.id)) ?? [], [data, scope.result]);
    const trailView = useMemo(() => {
        // Handtest K5: Pfad und Aufrufreihe auch in der Hierarchie eines Ausschnitts.
        if (!props.workspaceExpanded || (mode !== 'galaxy' && !scopedProjection) || trail?.key !== organicKey || !data || !scope.result) return undefined;
        const names = new Map(data.nodes.map(node => [node.id, graphNodeName(node)]));
        const nameOf = (id: number) => names.get(id) ?? `#${id}`;
        if (trail.kind === 'calls') {
            return { nameOf, lines: true, labels: 'active' as const, steps: rootCalls,
                heading: galaxyPathText.callsHeading(trailRoot === undefined ? '' : nameOf(trailRoot), rootCalls.length) };
        }
        const steps: ScopePathStep[] | undefined = shortestScopePath(data.edges, scope.result.roots, trail.target, scope.direction);
        const target = names.get(trail.target) ?? trail.name;
        return { nameOf, lines: false, labels: 'all' as const, steps: steps ?? [],
            heading: galaxyPathText.pathHeading(target, steps?.length ?? 0),
            note: steps === undefined ? galaxyPathText.noPath(target) : steps.length === 0 ? galaxyPathText.isRoot(target) : undefined };
    }, [props.workspaceExpanded, mode, trail, organicKey, data, scope.result, scope.direction, rootCalls, trailRoot]);
    const trailActive = trailView ? Math.min(trailStep, Math.max(0, trailView.steps.length - 1)) : 0;
    const trailIds = useMemo(() => trailView?.steps.length && scope.result ? pathNodes(trailView.steps, scope.result.roots) : undefined,
        [trailView, scope.result]);
    /*
     * Die Hierarchie zeichnet mit eigenen IDs (K5). Der Pfad rechnet im Scope und
     * wird fuer ihr Bild umgeschrieben, damit Linien, Ring und Abdunkeln an den
     * Knoten liegen, die dort zu sehen sind.
     */
    const hierarchyIds = useMemo(() => mode === 'hierarchy' && scopedProjection?.sourceIds
        ? new Map(scopedProjection.sourceIds.map((source, id) => [source, id])) : undefined, [mode, scopedProjection]);
    const sceneTrailIds = useMemo(() => !trailIds || !hierarchyIds ? trailIds
        : new Set([...trailIds].flatMap(id => hierarchyIds.get(id) ?? [])), [trailIds, hierarchyIds]);
    const scenePathSteps = useMemo(() => {
        if (!trailView) return undefined;
        if (!hierarchyIds) return trailView.steps;
        return trailView.steps.flatMap(step => {
            const from = hierarchyIds.get(step.from), to = hierarchyIds.get(step.to);
            const source = hierarchyIds.get(step.edge.source), target = hierarchyIds.get(step.edge.target);
            return from === undefined || to === undefined || source === undefined || target === undefined ? []
                : [{ edge: { ...step.edge, source, target }, from, to }];
        });
    }, [trailView, hierarchyIds]);
    const clearTrail = useCallback(() => { setTrail(undefined); setTrailStep(0); }, []);
    /*
     * Escape gibt den Pfad frei, aber erst nach allen, die vorgehen: liegt eine
     * Flaeche darueber (`escapeTaken`, dieselbe Reihenfolge wie beim
     * Vollbild), gehoert die Taste ihr, und wer in einem Feld oder im Editor
     * tippt, verlaesst mit Escape das Feld und nicht den Pfad.
     */
    const escapeTaken = props.escapeTaken === true;
    useEffect(() => {
        if (!trailView || escapeTaken) return;
        const onKey = (event: globalThis.KeyboardEvent): void => {
            if (event.key !== 'Escape' || event.defaultPrevented) return;
            if (isTypingTarget(event.target instanceof Element ? event.target : null)) return;
            event.preventDefault();
            clearTrail();
        };
        window.addEventListener('keydown', onKey);
        return () => window.removeEventListener('keydown', onKey);
    }, [trailView, escapeTaken, clearTrail]);

    const index = useMemo(
        () => (picture === undefined ? new Map<string, GraphNode>() : nodesByQualifiedName(picture.nodes)),
        [picture],
    );

    // Die Legende haengt am gezeigten Bild: ihre Kantenarten nennen die dieses
    // Graphen und nicht alle, die die Tabelle kennt.
    const legend = useMemo(
        () => (mode === 'hierarchy'
            ? hierarchyLegendEntries(picture, readerHierarchyActive, Boolean(scopedProjection))
            : galaxyLegendEntries(picture)),
        [mode, picture, readerHierarchyActive, scopedProjection],
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
    const fullscreen = visible && !props.workspaceExpanded && liveOn && agentPreference.fullscreen;

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
    const coverageShadow = useMemo(() => showCoverage && props.workspaceExpanded && mode === 'galaxy' && data
        ? buildCoverageShadow(data) : null, [data, props.workspaceExpanded, mode, showCoverage]);
    /*
     * Was eine neue Einpassung verlangt.
     *
     * In der gewaehlten Galaxie ist das der Scope selbst (Wurzel, Richtung,
     * Kantenarten) und nicht sein Bild: ein Expand, eine Vorschau und die
     * vollstaendige Antwort danach sind derselbe Ausschnitt, und eine Kamera,
     * die bei jedem davon neu einpasst, stellt den Leser jedes Mal anders hin
     * (Review-Befund G1). Wie weit die Kamera fuer das neue Bild stehen muss,
     * stellt die Szene selbst nach, entlang derselben Blickrichtung und nur,
     * solange der Leser die Kamera nicht bewegt hat (GraphScene,
     * `FitContainment`).
     */
    const fitScope = mode === 'galaxy' && props.workspaceExpanded && scope.scope ? organicKey : undefined;
    const fitPicture = fitScope === undefined ? picture : undefined;
    const fitProjection = fitScope === undefined ? projection : undefined;
    const fitRequest = useMemo(() => ({ scope: fitScope, picture: fitPicture, projection: fitProjection, mode, requestedFit, ownFit, coverageShadow }),
        [fitScope, fitPicture, fitProjection, mode, requestedFit, ownFit, coverageShadow]);
    const lastFitRequest = useRef<typeof fitRequest | undefined>(undefined);

    useEffect(() => {
        if (!visible) {
            return;
        }
        // Chat resizing changes the viewport, not the selected graph neighborhood.
        if (props.workspaceExpanded && lastFitRequest.current === fitRequest) return;
        /* Der Rahmenwechsel des Vollbilds passt nicht ein. Siehe `skipNextFit`. */
        if (skipNextFit.current) {
            skipNextFit.current = false;
            return;
        }
        if (mode === 'hierarchy' && projection !== undefined) {
            // In der Hierarchie eines Ausschnitts rahmt die Kamera die Namen in ihrer Breite (Runde 4, N1); ohne Namen zaehlt der Punkt.
            const box = hierarchyFrame(projection, projection === scopedProjection ? (placement) => {
                const node = projection.data.nodes[placement.id];
                return node === undefined || (scopedNamedIds !== undefined && !scopedNamedIds.has(placement.id)) ? 0 : hierarchyLabelWidth(graphNodeName(node));
            } : undefined);
            const target = computeFrameTarget(box, aspect);
            // Erst eine Einpassung, die wirklich lief, verbraucht die Anfrage:
            // ein Scope, dessen Bild noch angeordnet wird, passt danach ein.
            lastFitRequest.current = fitRequest;
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
        const nodes = [...(picture?.nodes ?? []), ...(coverageShadow?.nodes ?? [])];
        if (nodes.length === 0) {
            return;
        }
        /*
         * Die Wurzel steht in der Mitte (Review-Befund G4). Gerahmt wird um sie
         * herum, weit genug, dass auch der fernste Knoten passt; ohne das stand
         * die Mitte der Wolke im Bild und die Wurzel irgendwo darin.
         */
        const roots = fitScope === undefined ? [] : nodes.filter((node) => scope.result?.roots.has(node.id));
        const center = roots.length === 0 ? undefined : {
            x: roots.reduce((sum, node) => sum + node.x, 0) / roots.length,
            y: roots.reduce((sum, node) => sum + node.y, 0) / roots.length,
            z: roots.reduce((sum, node) => sum + node.z, 0) / roots.length,
        };
        const target = computeFitTarget(nodes, aspect, center);
        if (target === null) {
            return;
        }
        lastFitRequest.current = fitRequest;
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
            center: [fit.center.x, fit.center.y, fit.center.z],
        };
        // `scope.result` folgt dem Bild und loest selbst keine Einpassung aus.
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [visible, mode, projection, picture, aspect, requestedFit, ownFit, coverageShadow, fitRequest, props.workspaceExpanded, fitScope, scopedProjection, scopedNamedIds]);

    const [backgroundCleared, setBackgroundCleared] = useState(false);
    useEffect(() => {
        if (focusQualifiedName || stepQualifiedName || props.focusFilePath) setBackgroundCleared(false);
    }, [focusQualifiedName, stepQualifiedName, props.focusFilePath, props.focusSourceRange?.startLine, props.focusSourceRange?.endLine]);

    // Hin-Richtung in der Galaxie: das Twin-Subjekt zieht die Kamera nach.
    useEffect(() => {
        if (backgroundCleared || mode !== 'galaxy' || data === undefined) {
            return;
        }
        if (props.workspaceExpanded && scope.scope) {
            setHighlighted(new Set(data.nodes.map(node => node.id))); setNote(''); return;
        }
        const node = focusQualifiedName ? index.get(focusQualifiedName) : undefined;
        if (props.focusFilePath === '') {
            setHighlighted(null);
            setNote(GALAXY_NO_FOCUS_NOTE);
            return;
        }
        if (props.focusFilePath) {
            const focus = readerGraphFocus(data.nodes, props.focusFilePath, props.focusSourceRange);
            setHighlighted(focus.ids);
            setNote(focus.message);
            if (focus.ids.size > 0) flyTo(data.nodes, readerFocusFrame(focus.ids, data.edges), props.focusFilePath);
            return;
        }
        if (!focusQualifiedName) return;
        if (node === undefined) {
            setHighlighted(null);
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
    }, [backgroundCleared, mode, data, index, focusQualifiedName, flyTo, props.focusFilePath, props.focusSourceRange, aspect, visible, props.workspaceExpanded, scope.scope]);

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
     * draengt sich nicht vor (`escapeTaken` steht beim Pfad weiter oben).
     */
    useEffect(() => {
        const node = panel.current;
        if (node === null || !fullscreenIsolationRequired(fullscreen, escapeTaken)) {
            return;
        }
        return isolateFullscreenBackground(node);
    }, [fullscreen, escapeTaken]);
    useEffect(() => {
        if (!fullscreen || escapeTaken) {
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
    }, [fullscreen, escapeTaken, changeAgentPreference]);

    /**
     * Der Knoten, um den der Ring laeuft.
     *
     * Das Symbol vor dem Leser, wenn es im Bild vorkommt, sonst der Schritt,
     * auf dem die Fuehrung steht. In dieser Reihenfolge, weil ein Klick in das
     * Bild das Symbol wechselt, ohne die Fuehrung zu bewegen, und der Ring dann
     * dorthin gehoert, wo der Leser hingegangen ist.
     */
    const pulsedNode = useMemo(() => {
        if (backgroundCleared || mode !== 'hierarchy' || readerHierarchyActive) {
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
    }, [backgroundCleared, mode, readerHierarchyActive, focusQualifiedName, stepQualifiedName, index]);

    /*
     * Ob die Notiz zum Ring gilt (Runde 4, N2). Der Ring folgt dem Symbol, das
     * im Reader von Explore offen ist; die Notiz sagt, dass keiner der Knoten
     * dieses ist. Das gilt nur neben dem Reader: im Galaxy-Tab steht die
     * Wurzel eines Ausschnitts markiert in der Mitte, und Explore ist dort
     * nicht zu sehen. Ein Klick ins Leere blendet den Ring aus, macht die
     * Notiz aber nicht wahr; darum zaehlt hier das Bild und nicht der Ring.
     */
    const hierarchyNoFocusNote = useMemo(() => !props.workspaceExpanded
        && ![focusQualifiedName, stepQualifiedName].some((candidate) => candidate !== undefined && candidate.length > 0 && index.has(candidate))
        ? galaxyHierarchyNoteText.noFocus : '', [props.workspaceExpanded, focusQualifiedName, stepQualifiedName, index]);

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
        setNote(readerProjection ? readerProjection.message : hierarchyNoFocusNote);
    }, [mode, projection, pulsedNode, readerProjection, hierarchyNoFocusNote]);

    // Rueck-Richtung: ein Klick in die Szene oeffnet die Datei.
    const handleNodeClick = useCallback(
        (node: GraphNode) => {
            setBackgroundCleared(false);
            // In der Hierarchie bleibt die Kamera stehen und alles hell: sie
            // rahmt den ganzen Subgraphen, und auf eine Spalte zu zoomen waere
            // wieder die Nachbarschaftsansicht, gegen die dieses Bild gebaut
            // ist. Der Ring wandert, sobald das Symbol vor dem Leser wechselt.
            if (mode === 'galaxy' && picture !== undefined) {
                const ids = neighbourIds(node.id, picture.edges);
                setHighlighted(ids);
                flyTo(picture.nodes, ids, node.qualified_name ?? node.name);
            }
            const selected = layoutNodeForSelection(layout, node) ?? layoutNodeForSelection(data, node) ?? node;
            props.onSelectNode?.(selected);
            if (props.workspaceExpanded) {
                clearTrail();
                scope.select({ kind: 'node', id: selected.id, name: selected.name, qualifiedName: selected.qualified_name });
                setNote('');
                return;
            }
            const file = nodeFilePath(node);
            if (file === undefined || file.length === 0) {
                setNote(unopenableNodeNote(node));
                return;
            }
            setNote('');
            onOpenNode(selected);
        },
        [layout, data, picture, mode, flyTo, onOpenNode, props.onSelectNode, props.workspaceExpanded, scope.select, clearTrail],
    );

    /* "All graph" und Escape: der Ausschnitt wird verlassen, und das ist selbst ein Schritt im Verlauf (K2). */
    const leaveScope = useCallback(() => {
        setBackgroundCleared(true);
        if (props.workspaceExpanded) setChosenMode('galaxy');
        clearTrail();
        scope.reset();
        changeTraceTypes(undefined);
        setHighlighted(null);
        setNote(props.workspaceExpanded ? '' : mode === 'hierarchy' ? readerProjection?.message ?? hierarchyNoFocusNote : GALAXY_NO_FOCUS_NOTE);
        refitNow();
        props.onClearSelection?.();
    }, [mode, refitNow, props.onClearSelection, readerProjection, scope.reset, props.workspaceExpanded, changeTraceTypes, clearTrail, hierarchyNoFocusNote]);
    /*
     * Ein Klick ins Leere hebt im Ausschnitt nur die Markierung auf (Handtest
     * K9). Bis dahin verliess er den Ausschnitt samt Ebenen und Pfad, ohne
     * Rueckfrage, und ein Klick knapp neben einen Knoten genuegte. Aus dem
     * Ausschnitt fuehren jetzt nur "All graph", Escape und Zurueck; der
     * aufgehobene Pfad ist ein Schritt im Verlauf und kommt mit Zurueck wieder.
     */
    const handleBackgroundClick = useCallback(() => {
        if (props.workspaceExpanded && scope.scope) { clearTrail(); return; }
        leaveScope();
    }, [props.workspaceExpanded, scope.scope, clearTrail, leaveScope]);

    /*
     * Zurueck und Vor (Handtest K2), wie beim Blaettern.
     *
     * Der Eintrag ist die ganze Frage dieses Augenblicks: Wurzel, Tiefe,
     * Richtung, Kantenarten, offener Pfad und Ansicht, und nichts von der
     * Antwort. Er wird aus dem Zustand ABGELEITET und nicht an jeder Stelle
     * abgelegt, die ihn aendert; so kann kein Weg in einen neuen Ausschnitt den
     * Verlauf vergessen. Was ein Zurueck selbst herstellt, legt keinen neuen
     * Eintrag ab (`restoringKey`). Regeln und Grenzen: src/graph/navigation-history.ts
     * und docs/development/pr-2068-galaxy-history.md.
     */
    const historyTrail: ScopeTrail | undefined = trail?.key !== organicKey ? undefined
        : trail.kind === 'calls' ? { kind: 'calls' } : { kind: 'path', target: trail.target, name: trail.name };
    const historyEntry: GalaxyHistoryEntry | undefined = props.workspaceExpanded ? {
        ...(scope.scope ? { scope: scope.scope } : {}), depth: scope.depth, direction: scope.direction,
        ...(traceTypes ? { edgeTypes: traceTypes } : {}), ...(historyTrail ? { trail: historyTrail } : {}), mode,
    } : undefined;
    const historyKey = historyEntry ? galaxyHistoryOptions.key(historyEntry) : '';
    const latestEntry = useRef(historyEntry);
    latestEntry.current = historyEntry;
    const [history, setHistory] = useState(() => emptyNavigationHistory<GalaxyHistoryEntry>());
    const restoringKey = useRef<string | undefined>(undefined);
    const selectOnComplete = useRef<string | undefined>(undefined);
    const historyProject = useRef(project);
    useEffect(() => {
        const entry = latestEntry.current;
        /*
         * Ein Projektwechsel beginnt einen neuen Verlauf. Das Projekt kommt oft
         * erst nach dem ersten Bild aus der Adresszeile; der ganze Graph steht
         * dann schon da und muss der erste Eintrag bleiben. Ein offener
         * Ausschnitt gehoert noch zum alten Projekt und wird nicht uebernommen.
         */
        if (historyProject.current !== project) {
            historyProject.current = project;
            restoringKey.current = undefined;
            const fresh = emptyNavigationHistory<GalaxyHistoryEntry>();
            setHistory(entry !== undefined && !entry.scope ? pushNavigation(fresh, entry, galaxyHistoryOptions) : fresh);
            return;
        }
        if (entry === undefined) return;
        const expected = restoringKey.current;
        restoringKey.current = undefined;
        if (expected === historyKey) return;
        setHistory((current) => pushNavigation(current, entry, galaxyHistoryOptions));
    }, [historyKey, project]);
    const applyEntry = useCallback((entry: GalaxyHistoryEntry) => {
        scope.restore({ ...(entry.scope ? { scope: entry.scope } : {}), depth: entry.depth, direction: entry.direction });
        const types = entry.edgeTypes ? [...entry.edgeTypes] : undefined;
        changeTraceTypes(types);
        setChosenMode(entry.mode);
        const nextKey = JSON.stringify([project, entry.scope, entry.scope ? entry.direction : 'both', types]);
        setTrail(entry.trail ? { key: nextKey, ...entry.trail } : undefined);
        setTrailStep(0);
        setBackgroundCleared(false);
        setNote('');
        /*
         * Die Auswahl folgt der Wurzel: der ganze Graph hat keine, eine andere
         * Wurzel waehlt sich, sobald sie geladen ist (ein Symbol meldet sich
         * selbst, siehe `notifiedScopedSymbol`), und dieselbe Wurzel auf einer
         * anderen Tiefe oder nach einem Abbruch bleibt ausgewaehlt, mit ihren
         * "Selection details" und dem Kontext des Chats.
         */
        if (!entry.scope) { setHighlighted(null); props.onClearSelection?.(); }
        else if (!scope.scope || scopeIdentity(entry.scope) !== scopeIdentity(scope.scope))
            selectOnComplete.current = entry.scope.kind === 'symbol' ? undefined : scopeIdentity(entry.scope);
    }, [scope.restore, scope.scope, changeTraceTypes, project, props.onClearSelection]);
    const historyBack = peekNavigation(history, -1);
    const historyForward = peekNavigation(history, 1);
    const goHistory = useCallback((step: -1 | 1) => {
        const entry = peekNavigation(history, step);
        if (entry === undefined) return;
        restoringKey.current = galaxyHistoryOptions.key(entry);
        setHistory((current) => moveNavigation(current, step, galaxyHistoryOptions));
        applyEntry(entry);
    }, [history, applyEntry]);
    // A recent root is a new navigation: it is pushed and drops the forward branch.
    const jumpToRecent = useCallback((entry: GalaxyHistoryEntry) => applyEntry(entry), [applyEntry]);
    useEffect(() => {
        if (!scope.complete || !scopedRoot || !scope.scope || selectOnComplete.current !== scopeIdentity(scope.scope)) return;
        selectOnComplete.current = undefined;
        props.onSelectNode?.(layoutNodeForSelection(layout, scopedRoot) ?? scopedRoot);
    }, [scope.complete, scopedRoot, scope.scope, layout, props.onSelectNode]);
    useEffect(() => {
        if (!props.workspaceExpanded || escapeTaken) return;
        const onKey = (event: globalThis.KeyboardEvent): void => {
            if (event.defaultPrevented || !event.altKey || event.ctrlKey || event.metaKey || event.shiftKey) return;
            if (event.key !== 'ArrowLeft' && event.key !== 'ArrowRight') return;
            if (isTypingTarget(event.target instanceof Element ? event.target : null)) return;
            const step = event.key === 'ArrowLeft' ? -1 : 1;
            if (peekNavigation(history, step) === undefined) return;
            event.preventDefault();
            goHistory(step);
        };
        window.addEventListener('keydown', onKey);
        return () => window.removeEventListener('keydown', onKey);
    }, [props.workspaceExpanded, escapeTaken, history, goHistory]);
    /* Escape ohne offenen Pfad verlaesst den Ausschnitt (K9); den Pfad gibt der Griff weiter oben frei. */
    useEffect(() => {
        if (!props.workspaceExpanded || !scope.scope || trailView || escapeTaken) return;
        const onKey = (event: globalThis.KeyboardEvent): void => {
            if (event.key !== 'Escape' || event.defaultPrevented) return;
            if (isTypingTarget(event.target instanceof Element ? event.target : null)) return;
            event.preventDefault();
            leaveScope();
        };
        window.addEventListener('keydown', onKey);
        return () => window.removeEventListener('keydown', onKey);
    }, [props.workspaceExpanded, scope.scope, trailView, escapeTaken, leaveScope]);
    /* Handtest 2026-10-04 (A2): die Liste schliesst bei einem Klick daneben, mit Escape und mit jedem neuen Ort. */
    const recentMenu = useDismissibleMenu(historyKey);
    const historyControls = props.workspaceExpanded ? <span className="atlas-graph-history" role="group" aria-label={galaxyHistoryText.group}>
        <button type="button" aria-label={galaxyHistoryText.back} disabled={!historyBack} onClick={() => goHistory(-1)}
            title={historyBack ? galaxyHistoryText.backTo(historyEntryLabel(historyBack)) : galaxyHistoryText.noBack}>
            <FitLabel wide={galaxyHistoryText.backWide} narrow={galaxyHistoryText.backGlyph} /></button>
        <button type="button" aria-label={galaxyHistoryText.forward} disabled={!historyForward} onClick={() => goHistory(1)}
            title={historyForward ? galaxyHistoryText.forwardTo(historyEntryLabel(historyForward)) : galaxyHistoryText.noForward}>
            <FitLabel wide={galaxyHistoryText.forwardWide} narrow={galaxyHistoryText.forwardGlyph} /></button>
        {history.recent.length > 1 && <details className="atlas-graph-recent" ref={recentMenu.ref}>
            <summary title={galaxyHistoryText.recentTitle} aria-label={galaxyHistoryText.recent}>{galaxyHistoryText.recentGlyph}</summary>
            <ul className="atlas-graph-recent-menu" aria-label={galaxyHistoryText.recentList}>{history.recent.map((entry) => {
                const current = galaxyHistoryOptions.recentKey?.(entry) === (scope.scope ? scopeIdentity(scope.scope) : undefined);
                return <li key={galaxyHistoryOptions.recentKey?.(entry)}>
                    <button type="button" disabled={current} aria-current={current ? 'true' : undefined} onClick={() => {
                        recentMenu.close();
                        jumpToRecent(entry);
                    }}><strong>{entry.scope && scopeDisplayName(entry.scope)}</strong><span>{historyEntryDetail(entry)}</span></button>
                </li>;
            })}</ul>
        </details>}
    </span> : null;

    /* Der geladene Ausschnitt fuer "Selection details" (K13). */
    const selectionScope = useMemo<SelectionScope | undefined>(() => props.workspaceExpanded && scope.scope && scope.result ? {
        graph: scope.result.data, complete: scope.complete, direction: scope.direction, depth: scope.result.depth,
        ...(traceTypes ? { edgeTypes: traceTypes } : {}), ...(scope.result.partial ? { partial: scope.result.partial } : {}),
    } : undefined, [props.workspaceExpanded, scope.scope, scope.result, scope.complete, scope.direction, traceTypes]);
    const selectionContent = typeof props.selectionPanel === 'function' ? props.selectionPanel(selectionScope) : props.selectionPanel;

    /* K3 und Review: die Leiste misst, ob sie voll, knapp oder umgebrochen passt (toolbar-fit.tsx). */
    const explorationBar = useRef<HTMLDivElement>(null);
    useToolbarFit(explorationBar, Boolean(props.workspaceExpanded && scope.scope));

    /* Was die Leiste sonst noch braucht (K3): Quelle der Wurzel, Gruppen und was ausserhalb der Limits liegt. */
    const openRoot = props.workspaceExpanded && scopedRoot && nodeFilePath(scopedRoot)
        ? () => props.onOpenNode(layoutNodeForSelection(layout, scopedRoot) ?? scopedRoot) : undefined;
    /* Runde 4 (N1): die Wurzel heisst, wie die Galaxie sie zeigt; ein Branch-Knoten "django-demo · detached HEAD". */
    // Ordner und Dateien behalten ihren Pfad; ein Knoten nimmt seine Art von der geladenen Wurzel, wo es sie gibt.
    const rootShown = !scope.scope ? ''
        : (scope.scope.kind === 'node' || scope.scope.kind === 'symbol') && scopedRoot && scopedRoot.qualified_name === scope.scope.qualifiedName
            ? graphNodeName(scopedRoot) : scopeDisplayName(scope.scope);
    const groupCount = mode === 'galaxy' && organic ? organic.groups.length : 0;
    const groupsText = groupCount > 1 ? galaxyToolbarText.groupsTitle(groupCount) : undefined;
    const outsideLimits = props.workspaceExpanded && data && shown && (shown.nodes.length < data.nodes.length || shown.edges.length < data.edges.length)
        ? galaxyToolbarText.outsideLimits(data.nodes.length - shown.nodes.length, data.edges.length - shown.edges.length) : undefined;

    /*
     * "−" waehrend des Ladens ist ein Abbruch (K8), und ein Abbruch ist ein
     * Schritt zurueck und kein neuer: steht die vorige Ebene direkt davor im
     * Verlauf, geht es dorthin, und Vor laedt die abgebrochene Ebene wieder.
     */
    const cancelOrRemoveLayer = () => {
        const previous = historyEntry && { ...historyEntry, depth: scope.depth - 1 };
        if (scope.loading && previous && historyBack && galaxyHistoryOptions.key(historyBack) === galaxyHistoryOptions.key(previous)) goHistory(-1);
        else scope.setDepth(scope.depth - 1);
    };

    /* Was die Leiste ueber das Laden der Ebenen sagt (Handtest K8). */
    const partial = scope.complete ? scope.result?.partial : undefined;
    const scopeState = scope.validating ? 'checking' : scope.loading ? 'loading' : organicTask.loading ? 'arranging'
        : partial ? 'partial' : scope.complete ? 'complete' : 'preview';
    const scopeStatus = scope.validating ? galaxyLayerText.checking
        : scope.loading ? scope.progress ? galaxyLayerText.loadingProgress(scope.progress.layer, scope.progress.nodes, scope.progress.edges) : galaxyLayerText.loading(scope.depth)
            : organicTask.loading ? galaxyLayerText.arranging
                : scope.complete ? partial ? galaxyLayerText.partial(galaxyLayerText.counts(data?.nodes.length ?? 0, data?.edges.length ?? 0))
                    : `${galaxyLayerText.counts(data?.nodes.length ?? 0, data?.edges.length ?? 0)}${scope.result?.exhausted ? galaxyLayerText.endOfTrace : ''}`
                    : galaxyLayerText.partialPreview;
    const estimate = scope.complete ? nextLayerEstimate(scope.result) : undefined;
    // Die gezaehlten Aufrufe am Rand sind eine Untergrenze; das Wachstum allein uebersah die Knoten mit tausend Aufrufern (Review zu K8).
    const edgeCalls = estimate ? scope.edgeCalls : undefined;
    // G3: die Render-Limits gelten nur in der Werkzeugleiste des Galaxy-Arbeitsbereichs; das Mini-Galaxy laedt ohne sie.
    const expandOutlook: ExpandOutlook | undefined = estimate && {
        layer: estimate.layer, frontier: estimate.frontier, estimate: estimate.estimate, calls: edgeCalls,
        loaded: { nodes: data?.nodes.length ?? 0, edges: data?.edges.length ?? 0 },
        limits: props.workspaceExpanded ? { nodes: nodeBudget, edges: edgeBudget } : undefined,
    };
    const expandPast = expandOutlook && expandPastLimit(expandOutlook);
    const expandWarning = Boolean(expandPast);
    const expandBlocked = Boolean(scope.loading || scope.result?.exhausted || scope.result?.partial);
    // Review zu K31: gesperrt heisst immer mit Grund, auch waehrend eine Ebene laedt.
    const expandTitle = scope.loading ? galaxyLayerText.expandLoading(scope.depth)
        : partial ? galaxyLayerText.expandPartial : scope.result?.exhausted ? galaxyLayerText.expandEnd
        : expandOutlook ? galaxyLayerText.expandHint(expandOutlook, expandPast) : undefined;

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
    // Ein stehendes Bild ist kein Ladezustand: die Werkzeugleiste sagt, dass angeordnet wird.
    const layoutState = error.length > 0 ? 'failed' : sceneData === undefined ? 'loading' : 'ready';
    // Die Hierarchie braucht das Layout nicht: sie faerbt sich damit, sie lebt
    // nicht davon. Ein Walk, dessen Bild dasteht, ist fertig.
    const state = mode === 'hierarchy' ? projection ? 'ready'
        : readerHierarchyActive ? layoutState === 'ready' ? 'empty' : layoutState
        : props.focusWalkStatus === 'loading' ? 'loading' : props.focusWalkStatus === 'unavailable' ? 'failed' : 'empty' : layoutState;
    const headline =
        mode === 'hierarchy'
            ? projection ? readerProjection?.headline ?? hierarchyHeadline(projection, hierarchyOrigin)
                : readerHierarchyActive ? layoutState === 'loading' ? 'Loading file relationships…'
                    : layoutState === 'failed' ? `layout unavailable: ${error}`
                    : `${props.focusFilePath} is not in the loaded graph layout.`
                : props.focusWalkMessage || 'Choose a symbol to see its outgoing call hierarchy.'
            : layoutState === 'failed'
                ? `layout unavailable: ${error}`
                : layoutState === 'loading'
                    ? `loading the layout of ${project.length > 0 ? project : 'no project'} ...`
                    : layoutSummary((data ?? sceneData) as GraphData, LAYOUT_NODE_BUDGET);

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
            ? readerProjection?.edgeNote ?? hierarchyEdgeNote(projection.data.edges.length, indexEdges.length)
            : '',
        mode === 'hierarchy' && !readerHierarchyActive && walk === undefined && projection ? props.focusWalkMessage ?? '' : '',
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
            // Live: the scene publishes new name boxes from its frame loop, without a render of this panel.
            get labelBoxes() { return labelBoxes.current; },
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
            drawnEdges: sceneShown?.edges.length ?? 0,
            edgeNote,
            history: { index: history.index, entries: history.entries.map(historyEntryLabel), recent: history.recent.map(historyEntryLabel) },
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
                            ...(placement.side !== undefined ? { side: placement.side } : {}),
                            ...(placement.mixed ? { mixed: true } : {}),
                        })),
                        ...(projection.band ? { band: projection.band } : {}),
                        ...(projection.names ? { names: projection.names } : {}),
                        edges: projection.data.edges.map((edge) => ({
                            from: projection.data.nodes[edge.source]?.qualified_name
                                ?? projection.data.nodes[edge.source]?.name ?? '',
                            to: projection.data.nodes[edge.target]?.qualified_name
                                ?? projection.data.nodes[edge.target]?.name ?? '',
                        })),
                        walkEdges: readerHierarchyActive ? 0 : projection.data.edges.length,
                        extraEdges: readerHierarchyActive ? projection.data.edges.length : indexEdges.length,
                        extras: (readerHierarchyActive ? projection.data.edges : indexEdges).map((edge) => ({
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
        mode === 'hierarchy' && !backgroundCleared && pulsedNode !== undefined ? (
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
    /* K5: die Kantenarten an den Linien der Hierarchie eines Ausschnitts, solange kein Pfad seine eigenen zeigt. */
    // Review zu K5: Kantenschilder nur neben Namen; ohne Namen waeren sie Schilder an Punkten, die niemand zuordnen kann.
    const labelledEdges = useMemo(() => !sceneShown || !scopedNamedIds ? sceneShown?.edges
        : sceneShown.edges.filter(edge => scopedNamedIds.has(edge.source) && scopedNamedIds.has(edge.target)), [sceneShown, scopedNamedIds]);
    const hierarchyEdgeLabels = mode === 'hierarchy' && scopedProjection && !sceneTrailIds && sceneShown && labelledEdges?.length
        ? <HierarchyEdgeLabels nodes={sceneShown.nodes} edges={labelledEdges} layout={scopedSpots} nameBoxes={labelBoxes} /> : undefined;
    // Zweites Review zu K5: die Ueberschrift des Bandes der gemischt erreichten Knoten.
    const hierarchyBand = mode === 'hierarchy' && scopedProjection?.band ? <HierarchyBandLabel band={scopedProjection.band} /> : undefined;
    const overlay: ReactNode = (pulseRing === undefined && !liveOn && hierarchyEdgeLabels === undefined && hierarchyBand === undefined) ? undefined : (
        <>
            {pulseRing}
            {hierarchyEdgeLabels}
            {hierarchyBand}
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
                            const available = true;
                            const action = !visible
                                    ? 'open'
                                    : mode === candidate && candidate === 'galaxy' && !props.workspaceExpanded ? 'collapse' : 'switch';
                            return (
                                <Hint
                                    key={candidate}
                                    name={`graph-mode-${candidate}`}
                                    text={
                                        candidate === 'hierarchy' && projection === undefined
                                            ? props.workspaceExpanded ? galaxyHierarchyNoteText.unavailableWorkspace : galaxyHierarchyNoteText.unavailable
                                            : !visible
                                                ? graphModeCollapsedTitle(candidate)
                                                : mode === candidate && candidate === 'galaxy' && !props.workspaceExpanded
                                                    ? graphModeActiveTitle(candidate)
                                                    : candidate === 'galaxy'
                                                        ? 'galaxy: the whole project, laid out by the server'
                                                        : readerHierarchyActive ? 'hierarchy: incoming relationships, file definitions, and outgoing relationships'
                                                        : scopedProjection ? galaxyHierarchyText.hint(scope.direction, { mixed: scopedProjection.band?.count ?? 0, names: scopedNames,
                                                            budget: SCOPED_HIERARCHY_LABEL_BUDGET, neighbours: scopedNeighbours, sides: scopedNamedSides })
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
                                        /* Hierarchy stays actionable while its root is missing or loading. */
                                        aria-disabled={!available}
                                        onClick={() => {
                                            if (visible && mode === candidate && candidate === 'galaxy' && !props.workspaceExpanded) {
                                                props.onToggleVisible?.();
                                                return;
                                            }
                                            setChosenMode(candidate);
                                            if (props.workspaceExpanded && !scope.scope && !activeWalk) setNote('');
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
                    {props.onToggleVisible !== undefined && !props.workspaceExpanded && (
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
                {showCoverage && props.workspaceExpanded && !scope.scope && <span className="atlas-galaxy-coverage-key">
                    <i aria-hidden="true" />{mode === 'hierarchy' ? 'Coverage shadow is shown in galaxy view' : coverageShadow
                        ? `Coverage shadow: ${coverageShadow.counts.files} files, ${coverageShadow.counts.folders} folders with index gaps`
                        : data?.missed_graph ? 'No coverage-shadow nodes reported' : 'Coverage shadow unavailable in this layout'}
                </span>}
                <span
                    className="atlas-galaxy-headline"
                    hidden={Boolean(scope.scope)}
                    data-testid="atlas-galaxy-headline"
                    data-state={state}
                >
                    {props.workspaceExpanded && mode === 'galaxy' && state === 'ready' && data
                        ? `${shown?.nodes.length.toLocaleString() ?? 0} of ${data.total_nodes.toLocaleString()} nodes · ${shown?.edges.length.toLocaleString() ?? 0} relationships`
                        : headline}
                </span>
                {edgeNote.length > 0 && !scope.scope && (
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
            {(props.workspaceExpanded || scope.scope) && <div className="atlas-graph-exploration" aria-label="Graph scope" ref={explorationBar}
                data-scoped={props.workspaceExpanded && scope.scope ? 'true' : undefined}>
                {historyControls}
                {props.workspaceExpanded && <GalaxyNavigator embedded nodes={layout?.nodes ?? []} project={project} fetch={fetchImpl} onSelect={handleNodeClick} onSelectScope={next => { props.onClearSelection?.(); setBackgroundCleared(false); clearTrail(); scope.select(next); }} />}
                {scope.scope ? <>
                    {props.workspaceExpanded && <button type="button" onClick={leaveScope} aria-label={galaxyHistoryText.allGraph} title={galaxyHistoryText.allGraphTitle}>
                        <FitLabel wide={galaxyHistoryText.allGraph} narrow={galaxyHistoryText.allGraphNarrow} /></button>}
                    {/*
                      * Handtest K3: die Wurzel ist zugleich der Weg zu ihrer
                      * Quelle. Ein eigener Knopf "Open source" daneben kostete die
                      * Zeile bei offenem Chat rund hundert Pixel; er steht fuer
                      * die Tastatur weiter im Menue "⋯".
                      */}
                    {props.workspaceExpanded && openRoot
                        ? <button type="button" className="atlas-graph-scope-name" data-fit-whole="" onClick={openRoot} aria-label={galaxyToolbarText.openRootLabel(rootShown)}
                            title={galaxyToolbarText.openRootTitle(rootShown, scopedRoot?.file_path ?? '', scopedRoot?.start_line)}>{rootShown}</button>
                        : <strong className="atlas-graph-scope-name" data-fit-whole="" title={scopeTitle(scope.scope)}>{rootShown}</strong>}
                    {props.workspaceExpanded && <select aria-label="Trace direction" title={galaxyToolbarText.traceTitle} value={scope.direction}
                        onChange={event => scope.setDirection(event.target.value as 'both' | 'inbound' | 'outbound')}>
                        <option value="both">Both directions</option><option value="inbound">Incoming</option><option value="outbound">Outgoing</option>
                    </select>}
                    {props.workspaceExpanded && <TraceEdgeFilter kinds={traceKinds} availableTypes={kinds.map(kind => kind.type)} selected={traceTypes} onChange={changeTraceTypes} />}
                    {/*
                      * Handtest K8: waehrend eine Ebene laedt, ist "−" der Weg
                      * hinaus. Es bricht das Laden ab und steht sofort wieder auf
                      * der vorigen, schon vollstaendigen Ebene.
                      */}
                    <button type="button" disabled={scope.depth <= scope.minDepth}
                        title={scope.loading ? galaxyLayerText.cancelLoading(scope.depth) : galaxyLayerText.removeLayer}
                        onClick={cancelOrRemoveLayer} aria-label="Remove graph layer">−</button>
                    <span>{scope.depth} {scope.depth === 1 ? 'layer' : 'layers'}</span>
                    {/*
                      * G3: die Erklaerung zu Expand ist lang, und als natives
                      * `title` kam sie erst nach der Verzoegerung des Browsers.
                      * Der eigene Tooltip steht sofort bei Hover und Fokus unter
                      * dem Knopf, auch in der knappen Form "+1", ueber der Szene.
                      * Gesperrt heisst `aria-disabled` und nicht `disabled`: ein
                      * abgeschalteter Knopf nimmt weder Zeiger noch Fokus, und
                      * gerade dann (Ebene unvollstaendig, Ende des Traces) ist der
                      * Grund das, was der Leser wissen will.
                      */}
                    <Hint name="galaxy-expand" text={expandTitle}>
                        <button type="button" aria-disabled={expandBlocked || undefined}
                            data-warning={expandWarning || undefined}
                            onClick={() => { if (!expandBlocked) scope.setDepth(scope.depth + 1); }} aria-label={galaxyToolbarText.expand}>
                            <FitLabel wide={galaxyToolbarText.expand} narrow={galaxyToolbarText.expandNarrow} /></button>
                    </Hint>
                    {props.workspaceExpanded && (mode === 'galaxy' || scopedProjection) && <>
                        <PathPicker nodes={pathCandidates} onPick={node => { setTrail({ key: organicKey, kind: 'path', target: node.id, name: graphNodeName(node) }); setTrailStep(0); }} />
                        <button type="button" disabled={rootCalls.length === 0} aria-pressed={trail?.key === organicKey && trail.kind === 'calls'}
                            title={rootCalls.length ? galaxyPathText.callOrderTitle : galaxyPathText.callOrderUnavailable}
                            onClick={() => { if (trail?.key === organicKey && trail.kind === 'calls') clearTrail(); else { setTrail({ key: organicKey, kind: 'calls' }); setTrailStep(0); } }}>
                            <FitLabel wide={galaxyPathText.callOrder} narrow={galaxyPathText.callOrderNarrow} /></button>
                    </>}
                    <span className="atlas-graph-scope-count" role="status" data-state={scopeState}
                        title={partial ? galaxyLayerText.partialTitle(partial.layer, partial.limit === 'nodes' ? nodeBudget : edgeBudget, partial.limit)
                            : [scopeStatus, scope.loading && scope.progress ? galaxyLayerText.loadingRequest(scope.progress.requests) : groupsText]
                                .filter(Boolean).join('. ')}>{scopeStatus}</span>
                    {scope.error && <span className="atlas-graph-scope-warning" title={scope.error}>Some relationships could not be loaded. <button type="button" onClick={scope.retry}>Retry</button></span>}
                    {!props.workspaceExpanded && scope.complete && <small>All indexed direct dependencies included.</small>}
                    {!props.workspaceExpanded && groupCount > 1 && <small className="atlas-graph-scope-groups" title={groupsText}>{galaxyToolbarText.groups(groupCount)}</small>}
                </> : null}
                {props.workspaceExpanded && <>
                    {/*
                      * Im Scope tritt der Deckel in ein Aufklappfeld zurueck
                      * (Review-Befund G7): die Leiste traegt dort Pfad,
                      * Aufrufreihe und Zaehler, und bei 1600 Pixeln brach die
                      * Zeile sonst um. Ein Scope liegt fast immer unter dem
                      * Deckel; im ganzen Graphen bleibt er in der Zeile.
                      */}
                    {/* K3: im Ausschnitt stehen Quelle, Limits, Gruppen und abgeschnittene Knoten im Menue "⋯". */}
                    {scope.scope ? <details className="atlas-graph-limits atlas-graph-more" title={galaxyToolbarText.moreTitle}
                        data-attention={outsideLimits ? 'true' : undefined} onKeyDown={event => {
                            if (event.key === 'Escape') { event.stopPropagation(); event.currentTarget.open = false; }
                        }}>
                        <summary aria-label={galaxyToolbarText.more}>{galaxyToolbarText.moreGlyph}</summary>
                        <div className="atlas-graph-limits-menu">
                            {openRoot && <button type="button" onClick={openRoot}>{galaxyToolbarText.openSource}</button>}
                            {renderLimits}
                            {groupCount > 1 && <small className="atlas-graph-scope-groups" title={groupsText}>{galaxyToolbarText.groups(groupCount)}</small>}
                            {outsideLimits && <small>{outsideLimits}</small>}
                        </div>
                    </details> : renderLimits}
                    {mode === 'galaxy' && !scope.scope && <label className="atlas-graph-coverage-filter" title="Show files and folders with indexing gaps">
                        <input type="checkbox" aria-label="Show coverage graph" checked={showCoverage} onChange={event => setViewPreferences({ coverageShadow: event.target.checked })} />
                        Coverage
                    </label>}
                    {!scope.scope && outsideLimits && <small>{outsideLimits}</small>}
                </>}
            </div>}
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
                                            const hidden = props.workspaceExpanded && scope.scope
                                                ? traceTypes !== undefined && !traceTypes.includes(swatch.label)
                                                : hiddenKinds.has(swatch.label);
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
            <div className="atlas-galaxy-scene" data-testid="atlas-galaxy-scene" ref={scene}
                aria-busy={layoutLoading || scope.loading || organicTask.loading || spacingBusy}>
                <RenderProgress busy={visible && (layoutLoading || scope.loading || organicTask.loading || spacingBusy)}
                    {...(scope.scope && scope.loading ? { label: galaxyLayerText.previewLoading(scope.depth) } : {})} />
                {trailView && <PathSteps heading={trailView.heading} note={trailView.note} steps={trailView.steps} active={trailActive}
                    nameOf={trailView.nameOf} lines={trailView.lines} onStep={setTrailStep} onClear={clearTrail} />}
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
                {sceneShown !== undefined && everVisible.current && (
                    <GraphScene
                        active={visible}
                        separateNodes={mode === 'galaxy' && sceneScoped && sceneShown.nodes.length <= SCOPE_SEPARATION_LIMIT}
                        onRenderBusyChange={setSpacingBusy}
                        idleRotation={mode === 'galaxy' && !sceneScoped}
                        rootIds={mode === 'galaxy' && sceneScoped ? scope.result?.roots : undefined}
                        data={sceneShown}
                        display={display}
                        highlightedIds={sceneTrailIds ?? (scope.scope && scope.depth > 1 && mode === 'galaxy' ? null : highlighted)}
                        path={sceneTrailIds && trailView && scenePathSteps ? { steps: scenePathSteps, active: trailActive, labels: trailView.labels } : undefined}
                        emphasizeIncidentEdges={mode === 'galaxy' && Boolean(props.focusFilePath)}
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
                            mode === 'hierarchy' ? scopedProjection ? SCOPED_HIERARCHY_LABEL_MAX_TEXT_WIDTH : HIERARCHY_LABEL_MAX_TEXT_WIDTH : undefined
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
                         * stehen hoechstens sechzig Punkte (im Ausschnitt
                         * 150, Review zu K5), und eine Aufrufkette ohne
                         * Namen waere eine Reihe Punkte.
                         */
                        showLabels={
                            mode === 'hierarchy'
                                ? scopedProjection ? scopedNames === 'all' || Boolean(scopedNamedIds?.size) : sceneShown.nodes.length <= HIERARCHY_LABEL_BUDGET
                                : trailIds !== undefined || (highlighted !== null && highlighted.size > 0)
                        }
                        labelBudget={mode === 'hierarchy' && scopedProjection ? SCOPED_HIERARCHY_LABEL_BUDGET : undefined}
                        labelIds={mode === 'hierarchy' ? scopedNamedIds : undefined}
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
                        coverageShadow={coverageShadow}
                        onShadowNodeClick={(node) => {
                            if (coverageShadow) flyTo(coverageShadow.nodes, coverageShadow.ids, node.qualified_name ?? node.name);
                            props.onSelectShadowNode?.(node);
                            setNote(`${node.file_path ?? node.name}: coverage shadow. Detailed indexing reasons are not included in this layout.`);
                        }}
                        renderShadowTooltip={(node) => <HoverCardHtml position={[node.x, node.y, node.z]}>
                            <div className="atlas-coverage-tooltip"><b>{node.file_path ?? node.name}</b><p>Coverage shadow: not fully indexed.</p></div>
                        </HoverCardHtml>}
                        onBackgroundClick={handleBackgroundClick}
                        renderTooltip={(node) => <NodeTooltipCard node={node} />}
                        overlay={overlay}
                    />
                )}
                {/* Zweites Review zu K5: fehlen Namen, sagt das Bild es selbst, und wie man sie bekommt. */}
                {mode === 'hierarchy' && scopedProjection && scopedNames !== 'all' && (
                    <p className="atlas-hierarchy-key" data-testid="atlas-hierarchy-key">
                        {galaxyHierarchyText.namesNote(scope.direction, scopedProjection.data.nodes.length, scopedNames, scopedNeighbours, SCOPED_HIERARCHY_LABEL_BUDGET, scopedNamedSides)}
                    </p>
                )}
                {state !== 'ready' && (
                    <p className="atlas-galaxy-placeholder" data-state={state}>
                        {headline}
                        {mode === 'hierarchy' && state !== 'loading' && <><br /><button type="button" data-testid="atlas-hierarchy-choose-root" onClick={() => setChosenMode('galaxy')}>Choose a symbol in the galaxy</button></>}
                    </p>
                )}
                {/*
                  * Das Instrument liegt IM Kasten der Szene und nicht darunter:
                  * es erklaert, was auf dem Graphen zu sehen ist, und ein Kasten
                  * daneben waere eine zweite Flaeche, die man zwischen Bild und
                  * Text hin und her lesen muesste. Es ist deckend (Review zu
                  * K29) und faengt seine eigenen Klicks ab; der Rest der Flaeche
                  * bleibt die Szene.
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
            {selectionContent && <details className="galaxy-selection-evidence atlas-galaxy-selection-details" aria-label="Selection evidence">
                <summary>Selection details</summary>
                <div className="atlas-galaxy-selection-details-body">{selectionContent}</div>
            </details>}
            {note.length > 0 && !scope.loading && (
                <p className="atlas-galaxy-note" data-testid="atlas-galaxy-note">
                    {props.workspaceExpanded && note === GALAXY_NO_FOCUS_NOTE
                        ? 'Select a node or entry point to attach graph context to chat.' : note}
                </p>
            )}
        </section>
    );
}
